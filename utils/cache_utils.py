"""
This module provides the cache abstraction including a name space concept
implemented using https://code.google.com/p/memcached/wiki/NewProgrammingTricks
"""
import hashlib
import logging
import telnetlib
from collections import defaultdict
from functools import wraps
from inspect import stack
from uuid import uuid4

from aviso.settings import cache_con, cache_ttls, sec_context

from utils.misc_utils import (get_nested, pop_nested, recursive_dict_iter,
                              set_nested, try_index)

TEN_MINUTES = 60 * 10
TEN_HOURS = 60 * 10 * 60
default_ttl = cache_ttls.get('default', 60 * 60)

logger = logging.getLogger('gnana.%s' % __name__)

_PYCACHE = {}


def get_group_prefix(key, group):
    # Get current group prefix
    group_prefix = cache_con.cache.get('group.%s' % group)
    if not group_prefix:
        group_prefix = uuid4()
        cache_con.cache.set('group.%s' % group, group_prefix, time=cache_ttls.get(group, default_ttl))
    return "%s.%s" % (group_prefix, key)


# TODO: does this work with the each box has its own cache ... i think no, should use flags
def append_to_cache(key, value, group, delim='||', exp_time=None):
    if not cache_con.cache:
        return

    exp_time = exp_time if exp_time else cache_ttls.get(group, default_ttl)
    grp = get_group_prefix(key, group)
    if cache_con.cache.get(grp):
        return cache_con.cache.append(grp, delim + value, time=exp_time)
    return cache_con.cache.set(grp, value, time=exp_time)


def put_into_cache(key, value, group, totalsize=-1, chunker=None, exp_time=None, version=None):
    # If no cache is initialized, it is a no-op
    if not cache_con.cache:
        return

    # Put the value now
    try:
        exp_time = exp_time if exp_time else cache_ttls.get(group, default_ttl)
        grp = get_group_prefix(key, group)
        if version is not None:
            cache_success = cache_con.cache.set(grp, (value, version), time=exp_time)
        else:
            cache_success = cache_con.cache.set(grp, value, time=exp_time)
        if cache_success is False and chunker:
            chunk_keys = []
            for i, chunk in enumerate(chunker.chunk(value)):
                chunk_key = hashlib.md5(grp + '_chunk_{}'.format(i)).hexdigest()
                cache_con.cache.set(chunk_key, chunk, time=exp_time)
                chunk_keys.append(chunk_key)
            cache_con.cache.set(grp, {'__chunks__': chunk_keys, 'version': version}, time=exp_time)
    except Exception as ex:
        logger.warning('Unable to cache %s of size %s due to %s ', key, totalsize, str(ex))
        if not chunker:
            return
        logger.info('retrying cache set with chunker for key: %s', grp)
        try:
            chunk_keys = []
            for i, chunk in enumerate(chunker.chunk(value)):
                chunk_key = hashlib.md5(grp + '_chunk_{}'.format(i)).hexdigest()
                cache_con.cache.set(chunk_key, chunk, time=exp_time)
                chunk_keys.append(chunk_key)
            cache_con.cache.set(grp, {'__chunks__': chunk_keys, 'version': version}, time=exp_time)
        except Exception as e:
            logger.warn('failed cache set with chunker: %s', str(e))


def get_from_cache(key, group, chunker=None, raw_key=None, get_version=False):
    logger.debug("getting %s from memcache" % key)
    try:
        cache_key = get_group_prefix(key, group) if not raw_key else raw_key
        result = cache_con.cache.get(cache_key)
    except Exception as ex:
        logger.warning("unable to read %s due to %s", key, str(ex))
        result = None

    if result and (chunker and '__chunks__' in result):
        logger.info('dechunking cache get')
        version = result.get('version', 0)
        try:
            chunks = [cache_con.cache.get(chunk_key) for chunk_key in result['__chunks__']]
            value = chunker.dechunk(chunks)
        except Exception as e:
            logger.warn('failed to dechunk: %s, callstack: %s', str(e), stack())
            value = None

        if get_version:
            return value, version
        return value

    if get_version:
        if result is not None:
            try:
                value, version = result
            except Exception as e:
                logger.warn('failed to get cache version: %s, callstack %s', str(e), stack())
                value, version = None, 0
        else:
            value, version = None, 0
        return value, version
    else:
        return result


def remove_group(group):
    cache_con.cache.delete('group.%s' % group)


def remove_key(key, group, raw_flush=False):
    if raw_flush:
        cache_con.cache.delete(key)
    else:
        cache_con.cache.delete(get_group_prefix(key, group))


def remove_keys(keys, group, raw_flush=False):
    if raw_flush:
        cache_con.cache.delete_multi(keys)
    else:
        cache_con.cache.delete_multi([get_group_prefix(key, group) for key in keys])


def get_set_from_cache(key, group, fn):
    val = get_from_cache(key, group)
    if not val:
        val = fn()
        put_into_cache(key, val, group)
    return val


def remove_from_cache(func_name, *args, **kwargs):
    if not cache_con.cache:
        return
    key = make_cache_key(func_name, args, kwargs)
    try:
        remove_key(key, sec_context.details.name)
    except Exception as ex:
        logger.warning("unable to remove %s due to %s", func_name, str(ex))


def memcached(func):
    def cached_func(*args, **kwargs):
        try:
            key = make_cache_key(func.__name__, args, kwargs)
            ret_val = get_from_cache(key, sec_context.details.name)
            assert ret_val is not None
        except Exception as e:
            ret_val = func(*args, **kwargs)
            try:
                put_into_cache(key, ret_val, sec_context.details.name)
            except Exception as ee:
                pass

        return ret_val

    return cached_func


def make_cache_key(func_name, args, kwargs):
    key_data = f"{func_name}{[str(arg) for arg in args]}{[(str(k), str(v)) for k, v in sorted(kwargs.items())]}"
    return hashlib.md5(key_data.encode('utf-8')).hexdigest()

def memcacheable(cache_name, *cacheargs, **cache_kwargs):
    """
    decorator to add to a function or class to cache results
    caches result of call or instantiation in MEMCACHED

    MEMCACHED considerations:
    -   12/16/2018 update: each web node can have its own memcached server (or None)
        so we now use the tenant flags to track the latest version of the cache
        this also means the hit rate wont be 100% anymore 
        it will take some time to 'warm up' all the caches if the 10 hour expiration has passed

    -   memcached has a size limit for the values it can store of 1MB
        in the event you want to store something larger than that, there are two ways to try and get around it
        encoding:
            try to encode the value into something that takes less space
            pass an encoder class which has static 'encode' and 'decode' methods (ex HierEncoder)
        chunking:
            split value up into smaller chunks that will fit into cache
            pass a chunker class which has static 'chunk' and 'dechunk' methods (ex HierChunker)

    -   memcached is a {key: value} store, there is no nesting like {key: {inner_key: value}}
        the key for storing the value is constructed using the cache_name, and the function/class value of each cache_arg
        example:
            >>> @memcacheable('cachey', ('val1', 0), ('val2', 1))
            >>> def add_vals(val1, val2):
            >>>     return val1 + val2
            >>> add_vals(10, 5)
            the memcached key will be a hashed version of ['cachey', 10, 5] and the value will be 15

    Arguments
    ---------
    cache_name: str, required
        name of cache

    cacheargs: tuple, optional, (arg name, arg position)
        name that maps to argument in decorated function, used to make key for storing in cache
        and the position of that argument in function call

    encoder: class, optional
        class providing an encode and decode method

    chunker: class, optional
        class providing a chunk and dechunk method

    track_all: bool, optional
        if True, will also store all the cache keys for the cache_name in its own place, default False
    """

    def decorator(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            if kwargs.get('skip_cache'):
                return func(*args, **kwargs)

            if cache_kwargs.get('cache_rule'):
                rule_func, args_idxs = cache_kwargs['cache_rule']
                if not rule_func([try_index(args, args_idx) for args_idx in args_idxs]):
                    return func(*args, **kwargs)

            chunker = cache_kwargs.get('chunker')
            encoder = cache_kwargs.get('encoder')

            cache_args = get_cache_args(cacheargs, *args, **kwargs)
            latest_version = get_cache_version('memcache', [cache_name] + cache_args)
            key = make_cache_key(cache_name, cache_args, {})

            val, cached_version = get_from_cache(key, sec_context.details.name, chunker=chunker, get_version=True)
            if val is None or cached_version != latest_version:
                val = func(*args, **kwargs)
                cache_val = encoder.encode(val) if encoder else val
                if not latest_version:
                    latest_version = 1
                    incr_cache_version('memcache', [cache_name] + cache_args)
                put_into_cache(key, cache_val, sec_context.details.name, chunker=chunker, exp_time=TEN_HOURS,
                               version=latest_version)
                if cache_kwargs.get('track_all'):
                    track_key = make_cache_key(cache_name, [], {})
                    append_to_cache(track_key, ','.join(cache_args), sec_context.details.name, exp_time=TEN_HOURS)
            if encoder:
                try:
                    val = encoder.decode(val)
                except Exception as e:
                    #  TEMP: trying to figure out whats going on in memcached
                    logger.warn('failed to decode error: %s, with kwargs: %s', str(e), cache_kwargs)
                    val = func(*args, **kwargs)
            return val

        return wrapped

    return decorator


def clears_memcache(cache_name, *cacheargs, **cache_kwargs):
    """
    decorator to add to a function or class to clear cache
    clears results in MEMCACHED

    MEMCACHED considerations:
    -   memcached is a {key: value} store, there is no nesting like {key: {inner_key: value}}
        if you want to clear multiple keys at once you can use an arg_maker
        arg_maker:
            make more args out of the cache_args passed to clears_memcache
            pass an arg_maker class which has a static 'make' method (ex MonthMaker)

        OR if you are tracking all the keys you are storing, with track_key in memcacheable
        you can pass clear_all. this will wipe out everything in the cache for cache_name

    Arguments
    ---------
    cache_name: str, required
        name of cache

    cacheargs: tuple, optional, (arg name, arg position)
        name that maps to argument in decorated function, used to make key for storing in cache
        and the position of that argument in function call

    arg_maker: class, optional
        class providing a make method

    clear_all: bool, optional
        if True, will clear all values inside cache for cache_name, default False
    """

    def decorator(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            logger.info('clearing memcache for: %s', cache_name)
            val = func(*args, **kwargs)
            arg_maker = cache_kwargs.get('arg_maker')
            arg_func = cache_kwargs.get('arg_func')
            clear_all = cache_kwargs.get('clear_all')
            if clear_all:
                track_key = make_cache_key(cache_name, [], {})
                all_keys = get_from_cache(track_key, sec_context.details.name)
                clear_keys = [key.split(',') for key in all_keys.split('||')] if all_keys else []
            else:
                key_args = get_cache_args(cacheargs, *args, **kwargs)
                if arg_func:
                    key_args = [arg_func(arg) if arg else arg for arg in key_args]
                clear_keys = [key_args] if not arg_maker else [key_args + [arg] for arg in arg_maker.make(*key_args)]
            mem_keys = [make_cache_key(cache_name, clear_key, {}) for clear_key in clear_keys]
            remove_keys(mem_keys, sec_context.details.name)
            if not clear_all:
                for clear_key in clear_keys:
                    incr_cache_version('memcache', [cache_name] + clear_key)
            else:
                clear_cache_versions('memcache', cache_name)
            return val

        return wrapped

    return decorator


def pycacheable(cache_name, *cacheargs, **cache_kwargs):
    """
    decorator to add to a function or class to cache results
    caches result of call or instantiation in PYCACHE

    PYCACHE considerations:
    -   PYCACHE is just a globally scoped python dictionary
        since its a dictionary, you can nest keys {key: {inner_key: value}}
        the keys for storing the value is constructed using the cache_name, and the function/class value of each cache_arg
        example:
            >>> @pycacheable('cachey', ('val1', 0), ('val2', 1))
            >>> def add_vals(val1, val2):
            >>>     return val1 + val2
            >>> add_vals(10, 5)
            pycache will now look like {'cachey': {10: {5: 15}}}

    -   PYCACHE lives on the backend server
        since some of our environments have multiple boxes/thread, we have to do some work to keep them in sync
        ex if the cache is invalidated on server A, it better get invalidated on server B too
        pycache transparently handles this with cache versioning using the tenant flags as a store of the version

    -   the hit rate for pycache will be lower than memcached
        because of the multiple boxes/threads the hit rate between sessions will be around 25%
        within sessions, if a session val is passed or memcached is on, the hit rate should be 100%

    -   there is some overhead in checking the version number from the tenant flags
        so it is not optimal to use this for a quick calculation that you make 100s of times

    Arguments
    ---------
    cache_name: str, required
        name of cache

    cacheargs: tuple, optional, (arg name, arg position)
        name that maps to argument in decorated function, used to make key for storing in cache
        and the position of that argument in function call

    cache_kwargs: optional
        add a session variable to ensure cache hits in same session
        protects against not having memcached turn on in dev environments
    """

    def decorator(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            if kwargs.get('skip_cache'):
                return func(*args, **kwargs)
            ucache_name = str(cache_name)
            session = cache_kwargs.get('session', False)
            cache_args = get_cache_args(cacheargs, *args, **kwargs)
            cache_keys = [sec_context.name, ucache_name] + cache_args
            latest_version = get_cache_version('pycache', [ucache_name] + cache_args)
            cached_val, cached_version, cached_session = get_nested(_PYCACHE, cache_keys, (None, None, None))
            # regenerate val if:
            # no value in cache
            # no tenant flags found AND session doesnt match cached session
            # version in memcached doesnt match version in cache
            if cached_val is None or (
                    latest_version is False and session != cached_session) or cached_version != latest_version:
                if not latest_version:
                    latest_version = 1
                    incr_cache_version('pycache', [ucache_name] + cache_args)
                val = func(*args, **kwargs)
                set_nested(_PYCACHE, cache_keys, (val, latest_version, session))
            return get_nested(_PYCACHE, cache_keys)[0]

        return wrapped

    return decorator


def clears_pycache(cache_name, *cacheargs, **cache_kwargs):
    """
    decorator to remove items from cache

    PYCACHE considerations:
    -   PYCACHE is a dictionary, so instead of clearing value at a time, you can clear many levels at once
        example:
            >>> @pycacheable('cachey', ('Q', 0), ('P', 1))
            >>> def period_is_future(Q, P):
            >>>     return True
            >>> period_is_future('2019Q1', '201802')
            >>> period_is_future('2019Q1', '201803')
            pycache will now look like {'cachey': {'2019Q1': {'201802': True, '201803': True}}
            once it becomes 201803, 201802 is no longer true and so you could invalidate like
            >>> @clears_pycache('cachey', ('Q', 0), ('P', 1))
            >>> def period_is_past(Q, P):
            >>>     return
            >>> period_is_past('2019Q1', '201802')
            pycache will now be {'cachey': {'2019Q1': {'201803': True}}
            but if its now 2019Q2, you could also invalidate all of Q1 in one go
            >>> @clears_pycache('cachey', ('Q', 0))
            >>> def quarter_is_past(Q):
            >>>     return
            >>> quarter_is_past('2019Q1')
            pycache will now be {'cachey': {'2019Q1': {}}

    -   pycache versioning is saved in the tenant flags
        when a value is first saved, its version is set to 0
        when the cache gets cleared, the version number gets incremented in the flag
        when fetching from cache, we check that the cache version is not less than the latest version from the flag

    Arguments
    ---------
    cache_name: str, required
        name of cache

        cacheargs: tuple, optional, (arg name, arg position)
            name that maps to argument in decorated function, used to make key for storing in cache
            and the position of that argument in function call
    """

    def decorator(func):
        @wraps(func)
        def wrapped(*args, **kwargs):

            ucache_name = str(cache_name)
            ret_val = func(*args, **kwargs)
            cache_args = get_cache_args(cacheargs, *args, **kwargs)
            if cache_kwargs.get('clear_all'):
                # TODO: THIS clear all 
                pass
            arg_func = cache_kwargs.get('arg_func')
            if arg_func:
                cache_args = [arg_func(arg) if arg else arg for arg in cache_args]
            cache_keys = [sec_context.name, ucache_name] + cache_args
            pop_nested(_PYCACHE, cache_keys)
            incr_cache_version('pycache', [ucache_name] + cache_args)
            return ret_val

        return wrapped

    return decorator


def get_cache_args(cacheargs, *args, **kwargs):
    return [str(kwargs.get(k, try_index(args, i))) for k, i in cacheargs]


def get_cache_version(cache_type, cache_keys):
    cache_flag = sec_context.details.get_flag(cache_type, 'versions', {})
    if cache_type == 'pycache':
        search_keys = [x for x in cache_keys]
        while len(search_keys) > 1:
            try:
                latest_version = get_nested(cache_flag, search_keys, 0)
                if latest_version:
                    return latest_version
            except:
                pass
            search_keys.pop()
    return get_nested(cache_flag, cache_keys, 0)


def incr_cache_version(cache_type, cache_keys):
    # NOTE: this isnt concurrency safe, but will have an unnecessary cache miss instead of fetching stale data
    cache_flag = sec_context.details.get_flag(cache_type, 'versions', {})
    version = get_nested(cache_flag, cache_keys, 0)
    if isinstance(version, int):
        set_nested(cache_flag, cache_keys, version + 1)
    else:
        # TODO what if not a dict
        # actually got many cache keys, need to increment them all
        for path, val in recursive_dict_iter(version):
            if isinstance(version, int):
                set_nested(version, path, val + 1)
    sec_context.details.set_flag(cache_type, 'versions', cache_flag)


def clear_cache_versions(cache_type, cache_name):
    cache_flag = sec_context.details.get_flag(cache_type, 'versions', {})
    cache_flag.pop(cache_name, None)
    sec_context.details.set_flag(cache_type, 'versions', cache_flag)


def list_all_memcached_keys():
    keys = defaultdict(list)
    for cache_name, cache in cache_con.cache.__dict__.iteritems():
        if not cache:
            keys[cache_name] = None
            continue
        for cache_server in cache.servers:
            server, port = cache_server.address
            keys[cache_name].extend(get_all_memcached_keys(server, port))

    return keys


def get_all_memcached_keys(host='127.0.0.1', port=11211):
    if host == 'localserver':
        host = '127.0.0.1'
    t = telnetlib.Telnet(host, port)
    t.write('stats items STAT items:0:number 0 END\n')
    items = t.read_until('END').split('\r\n')
    keys = set()
    for item in items:
        parts = item.split(':')
        if not len(parts) >= 3:
            continue
        slab = parts[1]
        t.write(
            'stats cachedump {} 200000 ITEM views.decorators.cache.cache_header..cc7d9 [6 b; 1256056128 s] END\n'.format(
                slab))
        cachelines = t.read_until('END').split('\r\n')
        for line in cachelines:
            parts = line.split(' ')
            if not len(parts) >= 3:
                continue
            keys.add(parts[1])
    t.close()
    return keys