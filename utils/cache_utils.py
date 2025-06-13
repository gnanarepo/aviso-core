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
            logger.warning('failed cache set with chunker: %s', str(e))


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
            logger.warning('failed to dechunk: %s, callstack: %s', str(e), stack())
            value = None

        if get_version:
            return value, version
        return value

    if get_version:
        if result is not None:
            try:
                value, version = result
            except Exception as e:
                logger.warning('failed to get cache version: %s, callstack %s', str(e), stack())
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
                    logger.warning('failed to decode error: %s, with kwargs: %s', str(e), cache_kwargs)
                    val = func(*args, **kwargs)
            return val

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
