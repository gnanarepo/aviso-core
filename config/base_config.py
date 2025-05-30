import logging
from copy import deepcopy

# TODO: Replace this with the new Library import for Infra
# Reach out ot Waqas or Kuldeep for details.
from aviso.settings import sec_context 


from utils.date_utils import epoch

# TODO: Do we still need to support the backup and email?
# from api.tenantmanager import backup_and_mail_changes

AUDIT_COLL = 'audit_log'
from utils.common import cached_property

logger = logging.getLogger('gnana.%s' % __name__)


class BaseConfig:
    """
    base class for config objects

    Optional Arguments:
        config_json {dict} -- used to mock configs for tests
                              expected to be None in regular use
                              if None, will read config from db
                              (default: {None})
        test_mode {bool} -- skip trying to save config to db (default: {False})
        create_mode {bool} -- skip validating config before any has been set
                              (default: {False})
        debug {bool} -- log debugging info if True (default: {False})
        db {object} -- mongoclient connection, only used for unit tests (default: {None})
    """
    config_name = None

    def __init__(self, config_json=None, test_mode=False, create_mode=False, debug=False, db=None):
        self.test_mode = test_mode
        self.debug = debug
        self.db = db
        self.create_mode = create_mode
        self._raw_config, self.config = {}, {}
        if config_json is None:
            config_json = sec_context.details.get_config(category='micro_app', config_name=self.config_name)

        if not config_json:
            return

        self._raw_config = deepcopy(config_json)
        self.config = config_json
        if self.create_mode:
            return

        valid_config, warnings = self.validate(config_json)
        if not valid_config:
            self.config = None
            raise BadConfigurationError(warnings)

    @property
    def raw_config(self):
        """
        immutable json object of current config, straight from db

        Returns:
            dict -- config object
        """
        return self._raw_config

    def set_config(self, new_config):
        """
        set new config

        Arguments:
            new_config {dict} -- config dictionary

        Raises:
            Exception: warnings if config not valid
        """
        if not new_config:
            raise BadConfigurationError('no config provided')
        self._raw_config = new_config
        self.config = new_config
        valid_config, warnings = self.validate(new_config)

        if valid_config:
            if not self.test_mode:
                sec_context.details.set_config(category='micro_app',
                                               config_name=self.config_name,
                                               value=self.raw_config)
                sec_context.tenant_db[AUDIT_COLL].insert_one({'service': self.config_name,
                                                              'timestamp': epoch().as_epoch(),
                                                              'user': sec_context.login_user_name,
                                                              'action': 'set config',
                                                              'record': self.raw_config})

            self._invalidate_properties()
        else:
            self.config = None
            self._raw_config = None
            raise BadConfigurationError(warnings)

    def update_config(self, update_dict):
        """
        update config

        Arguments:
            update_dict {dict} -- updates to config to apply

        Raises:
            Exception: warnings if config not valid
        """
        if not update_dict:
            raise BadConfigurationError('no config provided')
        new_config = deepcopy(self.raw_config)
        new_config.update(update_dict)
        self._raw_config = new_config
        self.config = new_config
        valid_config, warnings = self.validate(new_config)

        tdetails = sec_context.details

        if valid_config:
            if not self.test_mode:
                tdetails.set_config(category='micro_app',
                                               config_name=self.config_name,
                                               value=self.raw_config)
                sec_context.tenant_db[AUDIT_COLL].insert_one({'service': self.config_name,
                                                              'timestamp': epoch().as_epoch(),
                                                              'user': sec_context.login_user_name,
                                                              'action': 'update config',
                                                              'record': self.raw_config})
                new_tdetails = sec_context.details
                new_tdetails = new_tdetails.get_all_config()

            self._invalidate_properties()
        else:
            self.config = None
            self._raw_config = None
            raise BadConfigurationError(warnings)

    def validate(self, config):
        """
        validate config

        Arguments:
            config {dict} -- config dictionary

        Returns:
            tuple -- (bool, [warnings])
        """
        raise NotImplementedError

    def _invalidate_properties(self):
        for key, value in self.__class__.__dict__.items():
            if isinstance(value, cached_property):
                self.__dict__.pop(key, None)


class BadConfigurationError(Exception):
    pass