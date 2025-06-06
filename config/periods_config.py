from config import BaseConfig
from utils.common import cached_property


class PeriodsConfig(BaseConfig):

    config_name = 'periods'

    @cached_property
    def monthly_predictions(self):
        """
        True if monthly predictions are enabled
        """
        return self.predict_period == 'M'

    @cached_property
    def monthly_fm(self):
        """
        True if monthly forecast management is enabled
        """
        return self.fm_period == 'M'

    @cached_property
    def weekly_fm(self):
        """
        True if weekly forecast management is enabled
        """
        #TODO to be eventually changed as below but isn't supported yet
        #return self.fm_period == 'W'
        return self.config.get('weekly_fm', False)

    @cached_property
    def is_custom_quarter_editable(self):
        """
        True if custom quarter editable is true
        """
        #TODO to be eventually changed as below but isn't supported yet
        #return self.fm_period == 'W'
        return self.config.get('is_custom_quarter_editable', False)

    @cached_property
    def predict_period(self):
        """
        period type predictions are made on

        Returns:
            str -- 'Q' for quarter or 'M' for months
        """
        return self.config.get('predict_period', 'Q')

    @cached_property
    def fm_period(self):
        """
        period type forecast management is done on

        Returns:
            str -- 'Q' for quarter or 'M' for months
        """
        return self.config.get('fm_period', 'Q')

    @cached_property
    def period_hidden(self):
        """
        Fetches the quarter before which previous year and quarter needs to be hidden
        """
        return self.config.get('period_hidden', None)

    @cached_property
    def yearly_fm(self):
        """
        True if yearly forecast management is enabled
        """
        return self.config.get('yearly_fm', False)

    @cached_property
    def use_sys_time(self):
        """
        use system time of app or maintain time with flags

        Returns:
            bool
        """
        return self.config.get('sys_time', True)

    @cached_property
    def future_periods_count(self):
        """
        number of future periods to display in the app

        Returns:
            int
        """
        return self.config.get('future_periods_count', 1)

    def validate(self, config):
        if not config:
            return True, ['no config provided']

        valid_periods_types = ['Q', 'M']
        try:
            assert config.get('predict_period', 'Q') in valid_periods_types
        except AssertionError:
            return False, ['Undefined period type for predict_period']

        try:
            assert config.get('fm_period', 'Q') in valid_periods_types
        except AssertionError:
            return False, ['Undefined period type for fm_period']

        try:
            assert not (config.get('predict_period', 'Q') == 'M'
                        and config.get('fm_period', 'Q') == 'Q')
        except:
            return False, ['Predicting for months, but FM only on quarters.']

        return True, []
