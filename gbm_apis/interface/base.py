from __future__ import print_function

from collections import namedtuple, defaultdict
import functools
import json
import logging
import re
import time

import six

# SDK clients still run Python 2 (aviso-sdk's setup.py branches on
# sys.version_info.major, and the gshell checkouts are py2), so this module has
# to import under both. urllib.parse does not exist on py2, and py2 unicode
# literals are not instances of str -- hence the aliases below, which the type
# declarations in the command metadata use.
if six.PY2:
    from urllib import urlencode
else:
    from urllib.parse import urlencode

# 'basestring'/'long' on py2, 'str'/'int' on py3. Taken from six rather than
# named directly so the py2 builtins are never referenced under py3.
basestring = six.string_types[0]
long = six.integer_types[-1]

from .shellDateUtils import EpochClass

logger = logging.getLogger()


class Argument:

    def __init__(self, name, arg_type=None, conflicts_with=None,
                 required=False, default=None, help=None,
                 requires_arguments=None, accept_list=False,
                 validations=None):
        """
        :param name: Name of the argument
        :param arg_type: Type of the argument
        :param conflicts_with: List of arguments not to be present, when this argument is present
        :param required: Use True to indicate it is required, or string to represent one from that named
            group to be required
        :param default: Default Value
        :param help: Helpful string
        :param requires_arguments: Additional arguments required by the presence of this argument
        :param accept_list: If this param accepts a list
        :param validations: Additional argument level validations

        :return:
        """
        self.name = name
        self.conflicts_with = conflicts_with
        self.required = required
        self.requires_arguments = requires_arguments
        self.arg_type = arg_type
        self.default = default
        self.help = help
        self.accept_list = accept_list
        self.validations = validations


CommandMeta = namedtuple('CommandMeta', [
    'help',         # Help text
    'name',         # Name of the command
    'arguments',    # List of arguments for the command
    'url_pattern',  # url_pattern to call.  Another named tuple. See Below
    'validations'   # Additional validation functions to be executed
])

url_meta_pattern = namedtuple('url_meta_pattern', [
    'url',
    'url_params',
    'postformat'
])


class GnanaSDKError(Exception):

    def __init__(self, message):
        self.details = {}
        #self.http_status = http_status
        if(isinstance(message, basestring)):
            logger.error(message)
            super(GnanaSDKError, self).__init__(message)
            self.details['_error'] = message
        elif(isinstance(message, dict)):
            self.details.update(message)
        else:
            self.details['_additional_info'] = message

        if(not '_error' in self.details):
            self.details['_error'] = self.__class__.__name__

    def get_error(self):
        self.details['_error']


class BaseSDKFunction(object):
    is_base = True
    default_command = None
    valid_shell_defaults = ('dataset', 'stage', 'file_type', 'model')

    def __init__(self):
        self.__name__ = self.__class__.__name__
        if isinstance(self.default_command, basestring):
            if self.default_command not in self.meta_commands:
                raise Exception('Default command not in the command list')

    def __call__(self, shell, *args, **kwargs):
        if len(args) == 0:
            if isinstance(self.default_command, basestring):
                command = self.default_command
            elif self.default_command:
                command = self.default_command(kwargs)
            else:
                raise GnanaSDKError(
                    'No command specified and no default_found')
        elif len(args) > 1:
            raise GnanaSDKError(
                'Not expecting any non keyword arguments beyond the command')
        else:
            command = args[0]

        # Special handling for help command to be available for all commands
        if command == 'help':
            if 'command' in kwargs:
                self.help(command=kwargs['command'])
            else:
                self.help()
            return

        if command == 'self':
            return self

        # Validate the command passed in
        command_metadata = self.meta_commands.get(command, None)
        if not command_metadata:
            raise GnanaSDKError(
                "Unknown command (%s) for this function" % command)

        required_groups = defaultdict(set)
        groups_found = set()
        all_args = list(kwargs.keys())
        special_args = [Argument(name='debug', arg_type=(bool,)),
                        Argument(name='api_profile', arg_type=(bool,)),
                        Argument(name='profile', arg_type=(basestring,)),
                        Argument(name='wait_for_pool', arg_type=(bool,)),
                        Argument(name='worker_pool', arg_type=(basestring,))]
        command_metadata.arguments.extend(special_args)
        for each_arg in command_metadata.arguments:
            arg_name = each_arg.name

            if arg_name in all_args:
                all_args.remove(arg_name)

            if isinstance(each_arg.required, basestring):
                required_groups[each_arg.required].add(arg_name)

            # This argument is not supplied. Check if a default can come from
            # some where
            if arg_name not in kwargs:

                # See if there is a default specified
                if each_arg.default is not None:
                    kwargs[arg_name] = each_arg.default() if callable(
                        each_arg.default) else each_arg.default
                # Look for defaults set with enter
                if arg_name in self.valid_shell_defaults:
                    val = getattr(shell, '_' + arg_name)
                    if val is not None:
                        kwargs[arg_name] = val
            if arg_name not in kwargs:
                if each_arg.required is True:
                    raise GnanaSDKError(
                        "Required argument %s is not found" % arg_name)
            else:
                if arg_name == 'stage' and kwargs[arg_name]:
                    if not re.match(r'^\w+$', kwargs[arg_name]):
                        raise GnanaSDKError(
                            "Don't use special characters in stage name. Please use alphanumeric only.")
                if isinstance(each_arg.required, basestring):
                    groups_found.add(each_arg.required)

                # Get the value
                value = kwargs[arg_name]

                # Validate the type
                if EpochClass == each_arg.arg_type[0]:
                    if isinstance(value, (long, int)):
                        pass
                    elif isinstance(value, EpochClass) or type(value).__name__ == 'EpochClass':
                        kwargs[arg_name] = value.as_epoch()
                    elif isinstance(value, float):
                        kwargs[arg_name] = EpochClass(value).as_epoch()
                    else:
                        error_msg = 'Unsupported Epoch value type type %s for %s' % (
                            type(value), arg_name)
                        logger.error(error_msg)
                        raise GnanaSDKError(error_msg)
                elif json == each_arg.arg_type[0]:
                    kwargs[arg_name] = json.dumps(value)
                elif value is None:
                    # Ignore None value
                    pass
                elif isinstance(value, each_arg.arg_type):
                    if each_arg.accept_list:
                        kwargs[arg_name] = [value]
                elif isinstance(value, (list, tuple, set)) and each_arg.accept_list:
                    for v in value:
                        if not isinstance(v, each_arg.arg_type):
                            raise GnanaSDKError('Value %s for argument %s must be one of the types %s' %
                                                (v, arg_name, str(each_arg.arg_type)))
                else:
                    raise GnanaSDKError('Argument %s must be one of the types %s' % (
                        arg_name, str(each_arg.arg_type)))

                # Check additional requirements
                if each_arg.requires_arguments:
                    for new_arg in each_arg.requires_arguments:
                        if new_arg not in kwargs:
                            raise GnanaSDKError(
                                'Argument %s requires %s to be present' % (arg_name, new_arg))

                # Check conflicts
                if each_arg.conflicts_with:
                    for new_arg in each_arg.conflicts_with:
                        if new_arg in kwargs and kwargs[new_arg] is not None:
                            raise GnanaSDKError(
                                'Argument %s can not be used with %s' % (arg_name, new_arg))

                # Argument Validations
                if each_arg.validations:
                    for v in each_arg.validations:
                        v(value)

        if all_args:
            logger.warn('Given arguments (%s) are not expected with this command' % str(all_args))

        for g in groups_found:
            required_groups.pop(g)

        for g in required_groups:
            raise GnanaSDKError(
                'One of (%s) argument is expected' % str(required_groups[g]))

        # Run through any additional command level validations
        if command_metadata.validations:
            for v in command_metadata.validations:
                v(kwargs)

        # After successful validation, we can dispatch the call
        command_fn = getattr(self.__class__,  command, None)
        if command_fn and callable(command_fn):
            return command_fn(self, shell, **kwargs)
        elif command_fn:
            raise GnanaSDKError(
                "Member exists but not callable.  Is this the intention?")
        else:
            # Try to perform the default implementation
            return self.call_url(shell, command=command, **kwargs)

    def call_url(self, shell, **kwargs):
        def processurl(qvar, args=[]):
            if isinstance(qvar, basestring):
                for arg in args:
                    qvar = qvar.replace('<' + arg + '>', (kwargs[arg] if isinstance(
                        kwargs[arg], basestring) else json.dumps(kwargs[arg])))
            return qvar

        def processquery(qvar):
            if isinstance(qvar, dict):
                for each in qvar:
                    qvar[each] = kwargs[qvar[each]]
            elif isinstance(qvar, list):
                mydict = {}
                for each in qvar:
                    mydict[each] = kwargs[each]
            else:
                logger.debug('Hey new format occured ,check it once')
            return qvar
        command = kwargs['command']
        print(command)
        kwargs['command'] = 'purge'
        command_pattern = self.meta_commands.get(command, None)
        if not command_pattern.url_pattern:
            #logger.error( "Seems meta data unfinished for  %s , please raise ticket for the same", command)
            raise GnanaSDKError(
                "please raise ticket ,Seems meta data unfinished for :" + command)
        url = command_pattern.url_pattern.url
        if re.search(r'\<(.*)\>', url):
            for each in re.search(r'\<(.*)\>', url).groups():
                url = url.replace('<' + each + '>', kwargs[each])
        url_params = command_pattern.url_pattern.url_params
        if url_params:
            print(url_params)
            for each in url_params.keys():
                print(each)
                print(url_params[each])
                if url_params[each] not in kwargs:
                    del url_params[each]
            url_params = processquery(url_params)
            if url_params:
                url += '?' + urlencode(url_params)
        # url.append(each)
        post = command_pattern.url_pattern.postformat
        if post:
            for each in post:
                if post[each] not in kwargs:
                    url_params.remove(each)
            post = processquery(post)
        print(url, ',', post)
        return shell.api(url, post)

    def help(self, command=None):
        """
        calling sdk_function('help'), lists the available commands and command level help
        calling sdk_function('help', command='xyz'), provides help for the given command
        along with all the arguments expected for that command

        :param command:
        :return:
        """
        if command:
            cmd_meta = self.meta_commands.get(command, None)
            if not cmd_meta:
                print('Such command not available at this momentS')
                return
            print(command, ':', cmd_meta.help, '\n')
            for arg in cmd_meta.arguments:
                print(arg.name, arg.arg_type)
                if arg.help:
                    print('\t ', arg.help)
        else:
            print("Available commands for this function are: \n")
            for each_cmd in self.meta_commands:
                print(each_cmd, ':', self.meta_commands[each_cmd].help)
            print("\n")
            print("Run the help function with command=<name> for help in specific command")
