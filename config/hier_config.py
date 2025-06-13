import json
import logging
import random
import string
from collections import defaultdict
from itertools import product

from aviso.settings import sec_context

from fake_data.fake_data import COMPANIES
from infra import HIER_COLL
from infra.mongo_utils import create_collection_checksum
from infra.read import get_period_and_close_periods, get_period_infos, get_as_of_dates, get_period_begin_end, \
    fetch_node_to_parent_mapping_and_labels
from utils.common import cached_property
from utils.date_utils import epoch
from .base_config import BaseConfig
import numpy as np


from utils.misc_utils import get_nested, is_lead_service, try_index
from .periods_config import PeriodsConfig

logger = logging.getLogger('gnana.%s' % __name__)


class DealFactory:
    def __init__(self, timestamp, period_infos, owner_ids, dd_owner_ids, seed=None):
        random.seed(seed)
        np.random.seed(seed)
        self.timestamp = timestamp
        self.period_infos = period_infos
        self.owner_ids = owner_ids
        self.dd_owner_ids = dd_owner_ids

        self.deal_types = ['new', 'renewal', 'services']

        self.open_stages = ['qualification', 'tech_validation', 'contracting']
        self.won_stage = 'closed_won'
        self.lost_stage = 'closed_lost'

        self.forecast_categories = ['upside', 'most likely', 'commit']
        self.stage_trans = [0, 60, 90]
        self.industry = ['finance', 'tech', 'poor']
        self.competitor = ['aviso', 'shittyOnes', 'nobody']

    def make_deals(self, count):
        deal_makers = {'c': [self.won,
                             self.lost,
                             self.future_dated_win,
                             self.overdue,
                             self.q_plus_one,
                             self.q_plus_two,
                             self.early_stage,
                             self.late_stage,
                             self.at_risk,
                             self.upside],
                       'h': [self.won,
                             self.lost,
                             self.overdue,
                             self.q_plus_one,
                             self.q_plus_two,
                             self.early_stage,
                             self.at_risk,
                             self.upside],
                       'f': [self.future_dated_win,
                             self.q_plus_one,
                             self.q_plus_two,
                             self.early_stage,
                             self.late_stage,
                             self.at_risk,
                             self.upside]}
        for i in range(count):
            owner_id = random.choice(self.owner_ids)
            dd_owner_id = self.dd_owner_ids.get(owner_id, owner_id)
            deal = {'opp_id': self.make_opp_id(i),
                    'owner_id': owner_id,
                    '__segs': [dd_owner_id],
                    'opp_name': random.choice(COMPANIES)}
            period_info = random.choice(self.period_infos)
            deal.update(random.choice(deal_makers[period_info['relative_period']])(period_info))
            yield deal

    def make_opp_id(self, x):
        rand_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        return ('00600' + str(x) + '00' + rand_part)[:15]

    def won(self, period_info):
        won_amount = random.randint(100, 1000)
        deal = {'stage': self.won_stage,
                'stage_prev': random.choice(self.open_stages),
                'win_prob': 1.0,
                'close_period': period_info['name'],
                'close_date': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'amount': won_amount,
                'amount_prev': random.randint(100, 1000),
                'amount_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'won_amount': won_amount,
                'forecast_category': self.forecast_categories[-1],
                'forecast_category_prev': random.choice(self.forecast_categories),
                'type': random.choice(self.deal_types),
                'stage_trans': 100,
                'stage_trans_prev': random.choice(self.stage_trans),
                'created_date': random.randint(period_info['begin'] - 100, period_info['begin']),
                'stage_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'close_date_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'forecast_category_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'industry': random.choice(self.industry),
                'competitor': random.choice(self.competitor),
                'terminal_fate': 'W'
                }
        deal['group_results'] = {u'values':
                                     {u'StageTrans_adj_ui_asof': deal['stage_trans'],
                                      u'CloseDate_qtr_bucket': 1},
                                 u'groups': {u'CloseDate_qtr_bucket~StageTrans_adj_ui_asof':
                                                 {u'2020Q4': {u'count': np.random.randint(low=50, high=200),
                                                              u'wins': np.random.randint(low=50, high=200),
                                                              u'mean': np.random.rand()},
                                                  u'2020M06': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()},
                                                  u'2020M05': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()}},
                                             u'CloseDate_qtr_bucket':
                                                 {u'2020Q4': {u'count': np.random.randint(low=50, high=200),
                                                              u'wins': np.random.randint(low=50, high=200),
                                                              u'mean': np.random.rand()},
                                                  u'2020M06': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()},
                                                  u'2020M05': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()}}}}

        return deal

    def lost(self, period_info):
        lost_amount = random.randint(100, 1000)
        deal = {'stage': self.lost_stage,
                'stage_prev': random.choice(self.open_stages),
                'win_prob': 0.0,
                'close_period': period_info['name'],
                'close_date': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'amount': lost_amount,
                'amount_prev': random.randint(100, 1000),
                'amount_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'lost_amount': lost_amount,
                'forecast_category': self.forecast_categories[0],
                'forecast_category_prev': random.choice(self.forecast_categories),
                'type': random.choice(self.deal_types),
                'stage_trans': -1,
                'stage_trans_prev': random.choice(self.stage_trans),
                'created_date': random.randint(period_info['begin'] - 100, period_info['begin']),
                'stage_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'close_date_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'forecast_category_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'industry': random.choice(self.industry),
                'competitor': random.choice(self.competitor),
                'terminal_fate': 'L'
                }
        deal['group_results'] = {u'values':
                                     {u'StageTrans_adj_ui_asof': deal['stage_trans'],
                                      u'CloseDate_qtr_bucket': 1},
                                 u'groups': {u'CloseDate_qtr_bucket~StageTrans_adj_ui_asof':
                                                 {u'2020Q4': {u'count': np.random.randint(low=50, high=200),
                                                              u'wins': np.random.randint(low=50, high=200),
                                                              u'mean': np.random.rand()},
                                                  u'2020M06': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()},
                                                  u'2020M05': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()}},
                                             u'CloseDate_qtr_bucket':
                                                 {u'2020Q4': {u'count': np.random.randint(low=50, high=200),
                                                              u'wins': np.random.randint(low=50, high=200),
                                                              u'mean': np.random.rand()},
                                                  u'2020M06': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()},
                                                  u'2020M05': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()}}}}

        return deal

    def future_dated_win(self, period_info):
        won_amount = random.randint(100, 1000)
        deal = {'stage': self.won_stage,
                'stage_prev': random.choice(self.open_stages),
                'win_prob': 1.0,
                'close_period': period_info['name'],
                'close_date': random.randint(self.timestamp, period_info['end']),
                'amount': won_amount,
                'amount_prev': random.randint(100, 1000),
                'amount_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'won_amount': won_amount,
                'forecast_category': self.forecast_categories[-1],
                'forecast_category_prev': random.choice(self.forecast_categories),
                'type': random.choice(self.deal_types),
                'stage_trans': 100,
                'stage_trans_prev': random.choice(self.stage_trans),
                'created_date': random.randint(period_info['begin'] - 100, period_info['begin']),
                'stage_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'close_date_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'forecast_category_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'industry': random.choice(self.industry),
                'competitor': random.choice(self.competitor),
                'terminal_fate': 'N/A'
                }
        deal['group_results'] = {u'values':
                                     {u'StageTrans_adj_ui_asof': deal['stage_trans'],
                                      u'CloseDate_qtr_bucket': 1},
                                 u'groups': {u'CloseDate_qtr_bucket~StageTrans_adj_ui_asof':
                                                 {u'2020Q4': {u'count': np.random.randint(low=50, high=200),
                                                              u'wins': np.random.randint(low=50, high=200),
                                                              u'mean': np.random.rand()},
                                                  u'2020M06': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()},
                                                  u'2020M05': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()}},
                                             u'CloseDate_qtr_bucket':
                                                 {u'2020Q4': {u'count': np.random.randint(low=50, high=200),
                                                              u'wins': np.random.randint(low=50, high=200),
                                                              u'mean': np.random.rand()},
                                                  u'2020M06': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()},
                                                  u'2020M05': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()}}}}

        return deal

    def overdue(self, period_info):
        deal = {'stage': self.open_stages[1],
                'stage_prev': random.choice(self.open_stages),
                'win_prob': random.randint(10, 20) / 100.,
                'close_period': period_info['name'],
                'close_date': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'amount': random.randint(100, 1000),
                'amount_prev': random.randint(100, 1000),
                'amount_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'forecast_category': self.forecast_categories[1],
                'forecast_category_prev': random.choice(self.forecast_categories),
                'type': random.choice(self.deal_types),
                'stage_trans': 60,
                'stage_trans_prev': random.choice(self.stage_trans),
                'created_date': random.randint(period_info['begin'] - 100, period_info['begin']),
                'stage_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'close_date_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'forecast_category_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'industry': random.choice(self.industry),
                'competitor': random.choice(self.competitor),
                'terminal_fate': 'N/A'
                }
        deal['group_results'] = {u'values':
                                     {u'StageTrans_adj_ui_asof': deal['stage_trans'],
                                      u'CloseDate_qtr_bucket': 1},
                                 u'groups': {u'CloseDate_qtr_bucket~StageTrans_adj_ui_asof':
                                                 {u'2020Q4': {u'count': np.random.randint(low=50, high=200),
                                                              u'wins': np.random.randint(low=50, high=200),
                                                              u'mean': np.random.rand()},
                                                  u'2020M06': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()},
                                                  u'2020M05': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()}},
                                             u'CloseDate_qtr_bucket':
                                                 {u'2020Q4': {u'count': np.random.randint(low=50, high=200),
                                                              u'wins': np.random.randint(low=50, high=200),
                                                              u'mean': np.random.rand()},
                                                  u'2020M06': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()},
                                                  u'2020M05': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()}}}}

        return deal

    def q_plus_one(self, period_info):
        deal = {'stage': self.open_stages[0],
                'stage_prev': random.choice(self.open_stages),
                'win_prob': random.randint(1, 10) / 100.,
                'close_period': period_info['plus_1_name'],
                'close_date': random.randint(period_info['plus_1_begin'], period_info['plus_1_end']),
                'amount': random.randint(100, 1000),
                'amount_prev': random.randint(100, 1000),
                'amount_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'forecast_category': self.forecast_categories[0],
                'forecast_category_prev': random.choice(self.forecast_categories),
                'type': random.choice(self.deal_types),
                'stage_trans': 10,
                'stage_trans_prev': random.choice(self.stage_trans),
                'created_date': random.randint(period_info['begin'] - 100, period_info['begin']),
                'stage_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'close_date_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'forecast_category_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'industry': random.choice(self.industry),
                'competitor': random.choice(self.competitor),
                'terminal_fate': 'N/A'
                }
        deal['group_results'] = {u'values':
                                     {u'StageTrans_adj_ui_asof': deal['stage_trans'],
                                      u'CloseDate_qtr_bucket': 1},
                                 u'groups': {u'CloseDate_qtr_bucket~StageTrans_adj_ui_asof':
                                                 {u'2020Q4': {u'count': np.random.randint(low=50, high=200),
                                                              u'wins': np.random.randint(low=50, high=200),
                                                              u'mean': np.random.rand()},
                                                  u'2020M06': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()},
                                                  u'2020M05': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()}},
                                             u'CloseDate_qtr_bucket':
                                                 {u'2020Q4': {u'count': np.random.randint(low=50, high=200),
                                                              u'wins': np.random.randint(low=50, high=200),
                                                              u'mean': np.random.rand()},
                                                  u'2020M06': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()},
                                                  u'2020M05': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()}}}}

        return deal

    def q_plus_two(self, period_info):
        deal = {'stage': self.open_stages[0],
                'stage_prev': random.choice(self.open_stages),
                'win_prob': random.randint(1, 5) / 100.,
                'close_period': period_info['plus_2_name'],
                'close_date': random.randint(period_info['plus_2_begin'], period_info['plus_2_end']),
                'amount': random.randint(100, 1000),
                'amount_prev': random.randint(100, 1000),
                'stage_trans_prev': random.choice(self.stage_trans),
                'amount_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'forecast_category': self.forecast_categories[0],
                'forecast_category_prev': random.choice(self.forecast_categories),
                'type': random.choice(self.deal_types),
                'stage_trans': 10,
                'created_date': random.randint(period_info['begin'] - 100, period_info['begin']),
                'stage_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'close_date_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'forecast_category_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'industry': random.choice(self.industry),
                'competitor': random.choice(self.competitor),
                'terminal_fate': 'N/A'
                }
        deal['group_results'] = {u'values':
                                     {u'StageTrans_adj_ui_asof': deal['stage_trans'],
                                      u'CloseDate_qtr_bucket': 1},
                                 u'groups': {u'CloseDate_qtr_bucket~StageTrans_adj_ui_asof':
                                                 {u'2020Q4': {u'count': np.random.randint(low=50, high=200),
                                                              u'wins': np.random.randint(low=50, high=200),
                                                              u'mean': np.random.rand()},
                                                  u'2020M06': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()},
                                                  u'2020M05': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()}},
                                             u'CloseDate_qtr_bucket':
                                                 {u'2020Q4': {u'count': np.random.randint(low=50, high=200),
                                                              u'wins': np.random.randint(low=50, high=200),
                                                              u'mean': np.random.rand()},
                                                  u'2020M06': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()},
                                                  u'2020M05': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()}}}}

        return deal

    def early_stage(self, period_info):
        deal = {'stage': self.open_stages[0],
                'stage_prev': random.choice(self.open_stages),
                'win_prob': random.randint(20, 30) / 100.,
                'close_period': period_info['name'],
                'close_date': period_info['end'],
                'amount': random.randint(100, 1000),
                'amount_prev': random.randint(100, 1000),
                'amount_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'forecast_category': self.forecast_categories[0],
                'forecast_category_prev': random.choice(self.forecast_categories),
                'type': random.choice(self.deal_types),
                'stage_trans': 10,
                'stage_trans_prev': random.choice(self.stage_trans),
                'created_date': random.randint(period_info['begin'] - 100, period_info['begin']),
                'stage_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'close_date_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'forecast_category_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'industry': random.choice(self.industry),
                'competitor': random.choice(self.competitor),
                'terminal_fate': 'N/A'
                }
        deal['group_results'] = {u'values':
                                     {u'StageTrans_adj_ui_asof': deal['stage_trans'],
                                      u'CloseDate_qtr_bucket': 1},
                                 u'groups': {u'CloseDate_qtr_bucket~StageTrans_adj_ui_asof':
                                                 {u'2020Q4': {u'count': np.random.randint(low=50, high=200),
                                                              u'wins': np.random.randint(low=50, high=200),
                                                              u'mean': np.random.rand()},
                                                  u'2020M06': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()},
                                                  u'2020M05': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()}},
                                             u'CloseDate_qtr_bucket':
                                                 {u'2020Q4': {u'count': np.random.randint(low=50, high=200),
                                                              u'wins': np.random.randint(low=50, high=200),
                                                              u'mean': np.random.rand()},
                                                  u'2020M06': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()},
                                                  u'2020M05': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()}}}}

        return deal

    def late_stage(self, period_info):
        deal = {'stage': self.open_stages[-1],
                'stage_prev': random.choice(self.open_stages),
                'win_prob': random.randint(75, 85) / 100.,
                'close_period': period_info['name'],
                'close_date': random.randint(self.timestamp, period_info['end']),
                'amount': random.randint(100, 1000),
                'amount_prev': random.randint(100, 1000),
                'amount_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'forecast_category': self.forecast_categories[-1],
                'forecast_category_prev': random.choice(self.forecast_categories),
                'type': random.choice(self.deal_types),
                'stage_trans': 95,
                'stage_trans_prev': random.choice(self.stage_trans),
                'created_date': random.randint(period_info['begin'] - 100, period_info['begin']),
                'stage_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'close_date_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'forecast_category_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'industry': random.choice(self.industry),
                'competitor': random.choice(self.competitor),
                'terminal_fate': 'N/A'
                }
        deal['group_results'] = {u'values':
                                     {u'StageTrans_adj_ui_asof': deal['stage_trans'],
                                      u'CloseDate_qtr_bucket': 1},
                                 u'groups': {u'CloseDate_qtr_bucket~StageTrans_adj_ui_asof':
                                                 {u'2020Q4': {u'count': np.random.randint(low=50, high=200),
                                                              u'wins': np.random.randint(low=50, high=200),
                                                              u'mean': np.random.rand()},
                                                  u'2020M06': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()},
                                                  u'2020M05': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()}},
                                             u'CloseDate_qtr_bucket':
                                                 {u'2020Q4': {u'count': np.random.randint(low=50, high=200),
                                                              u'wins': np.random.randint(low=50, high=200),
                                                              u'mean': np.random.rand()},
                                                  u'2020M06': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()},
                                                  u'2020M05': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()}}}}

        return deal

    def at_risk(self, period_info):
        deal = {'stage': self.open_stages[0],
                'stage_prev': random.choice(self.open_stages),
                'win_prob': random.randint(0, 10) / 100.,
                'close_period': period_info['name'],
                'close_date': period_info['end'],
                'amount': random.randint(100, 1000),
                'amount_prev': random.randint(100, 1000),
                'amount_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'forecast_category': self.forecast_categories[-1],
                'forecast_category_prev': random.choice(self.forecast_categories),
                'type': random.choice(self.deal_types),
                'stage_trans': 10,
                'stage_trans_prev': random.choice(self.stage_trans),
                'created_date': random.randint(period_info['begin'] - 100, period_info['begin']),
                'stage_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'close_date_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'forecast_category_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'industry': random.choice(self.industry),
                'competitor': random.choice(self.competitor),
                'terminal_fate': 'N/A'
                }
        deal['group_results'] = {u'values':
                                     {u'StageTrans_adj_ui_asof': deal['stage_trans'],
                                      u'CloseDate_qtr_bucket': 1},
                                 u'groups': {u'CloseDate_qtr_bucket~StageTrans_adj_ui_asof':
                                                 {u'2020Q4': {u'count': np.random.randint(low=50, high=200),
                                                              u'wins': np.random.randint(low=50, high=200),
                                                              u'mean': np.random.rand()},
                                                  u'2020M06': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()},
                                                  u'2020M05': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()}},
                                             u'CloseDate_qtr_bucket':
                                                 {u'2020Q4': {u'count': np.random.randint(low=50, high=200),
                                                              u'wins': np.random.randint(low=50, high=200),
                                                              u'mean': np.random.rand()},
                                                  u'2020M06': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()},
                                                  u'2020M05': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()}}}}

        return deal

    def upside(self, period_info):
        deal = {'stage': self.open_stages[1],
                'stage_prev': random.choice(self.open_stages),
                'win_prob': random.randint(50, 70) / 100.,
                'close_period': period_info['name'],
                'close_date': period_info['end'],
                'amount': random.randint(100, 1000),
                'amount_prev': random.randint(100, 1000),
                'amount_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'forecast_category': self.forecast_categories[0],
                'forecast_category_prev': random.choice(self.forecast_categories),
                'type': random.choice(self.deal_types),
                'stage_trans': 60,
                'stage_trans_prev': random.choice(self.stage_trans),
                'created_date': random.randint(period_info['begin'] - 100, period_info['begin']),
                'stage_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'close_date_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'forecast_category_lud': random.randint(self.timestamp - random.randint(5, 50), self.timestamp - 5),
                'industry': random.choice(self.industry),
                'competitor': random.choice(self.competitor),
                'terminal_fate': 'N/A'
                }
        deal['group_results'] = {u'values':
                                     {u'StageTrans_adj_ui_asof': deal['stage_trans'],
                                      u'CloseDate_qtr_bucket': 1},
                                 u'groups': {u'CloseDate_qtr_bucket~StageTrans_adj_ui_asof':
                                                 {u'2020Q4': {u'count': np.random.randint(low=50, high=200),
                                                              u'wins': np.random.randint(low=50, high=200),
                                                              u'mean': np.random.rand()},
                                                  u'2020M06': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()},
                                                  u'2020M05': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()}},
                                             u'CloseDate_qtr_bucket':
                                                 {u'2020Q4': {u'count': np.random.randint(low=50, high=200),
                                                              u'wins': np.random.randint(low=50, high=200),
                                                              u'mean': np.random.rand()},
                                                  u'2020M06': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()},
                                                  u'2020M05': {u'count': np.random.randint(low=50, high=200),
                                                               u'mean': np.random.rand()}}}}

        return deal

    def rewind_deal(self, deal, timestamp):
        new_deal = {k: v for k, v in deal.items()}
        if deal.get('update_date') == timestamp:
            return new_deal

        # was also closed at other time, dont change
        if deal['stage'] in [self.won_stage, self.lost_stage] and deal['close_date'] <= timestamp:
            return new_deal

        # unwin
        if deal['stage'] == self.won_stage:
            new_deal['stage'] = self.open_stages[-1]
            new_deal['win_prob'] = random.randint(75, 85) / 100.
            new_deal.pop('won_amount')
            return new_deal

        # unlose
        if deal['stage'] == self.lost_stage:
            new_deal['stage'] = self.open_stages[1]
            new_deal['win_prob'] = random.randint(1, 10) / 100.
            new_deal.pop('lost_amount')
            return new_deal

        # deal is stalling
        if deal['win_prob'] <= .3:
            new_deal['close_date'] = new_deal['close_date'] - 7
            new_deal['win_prob'] += .05
            fc_idx = self.forecast_categories.index(new_deal['forecast_category'])
            try:
                new_deal['forecast_category'] = self.forecast_categories[fc_idx + 1]
            except IndexError:
                pass
            return new_deal

        # deal is progressing normally
        stg_idx = self.open_stages.index(new_deal['stage'])
        fc_idx = self.forecast_categories.index(new_deal['forecast_category'])
        new_deal['win_prob'] -= .05
        try:
            new_deal['stage'] = self.open_stages[stg_idx - 1]
        except IndexError:
            pass
        try:
            new_deal['forecast_category'] = self.forecast_categories[fc_idx - 1]
        except IndexError:
            pass
        if random.choice([True, False]):
            new_deal['close_date'] = new_deal['close_date'] + 7

        return new_deal

    def fastforward_deal(self, deal, timestamp):
        new_deal = {k: v for k, v in deal.items()}
        if deal.get('update_date') == timestamp:
            return new_deal

        # deal is still closed
        if deal['stage'] in [self.won_stage, self.lost_stage]:
            return new_deal

        # win a deal
        if deal['win_prob'] >= .7 and deal['stage'] == self.open_stages[-1] and random.randint(0, 1) >= .7:
            new_deal['stage'] = self.won_stage
            new_deal['win_prob'] = 1.0
            new_deal['close_date'] = timestamp
            new_deal['won_amount'] = new_deal['amount']
            return new_deal

        # lose a deal
        if deal['win_prob'] <= .2 and deal['stage'] == self.open_stages[-1] and random.randint(0, 1) >= .7:
            new_deal['stage'] = self.lost_stage
            new_deal['win_prob'] = 0.0
            new_deal['close_date'] = timestamp
            new_deal['lost_amount'] = 0.0
            return new_deal

        # deal is stalling
        if deal['win_prob'] <= .3:
            new_deal['close_date'] = new_deal['close_date'] + 7
            new_deal['win_prob'] -= .05
            fc_idx = self.forecast_categories.index(new_deal['forecast_category'])
            try:
                new_deal['forecast_category'] = self.forecast_categories[fc_idx - 1]
            except IndexError:
                pass
            return new_deal

        # deal is progressing normally
        stg_idx = self.open_stages.index(new_deal['stage'])
        new_deal['win_prob'] += .05
        try:
            new_deal['stage'] = self.open_stages[stg_idx + 1]
        except IndexError:
            pass
        if random.choice([True, False]):
            new_deal['close_date'] = new_deal['close_date'] - 7

        return new_deal


def get_distinct_deal_field_values(period, deal_fields, deal_count=None, seed=None):
    deal_count = deal_count or 100
    period_infos = get_period_infos(period)
    as_ofs = get_as_of_dates(period)
    now_xl, now_ep = get_nested(as_ofs, ['now', 'xl']), get_nested(as_ofs, ['now', 'ep'])

    owner_ids = [None]
    dd_owner_ids = {}

    deal_factory = DealFactory(int(now_xl), period_infos, owner_ids, dd_owner_ids, seed)

    woot = defaultdict(set)
    for deal in deal_factory.make_deals(deal_count):
        for df in deal_fields:
            woot[df].add(deal.get(df, 'N/A'))

    return {'|'.join(deal_fields): [zip(deal_fields, vals)
                                    for vals in product(*[woot[field] for field in deal_fields])]}


class HierarchyBuilder:
    """
    base class for hierarchy builders
    need to adhere to contract of having build_hierarchy method
    """

    def __init__(self, period):
        self.config = HierConfig()
        self.period = period

    def build_hierarchy(self):
        """
        builds new hierarchy from whatever source system tenant uses

        Returns:
            tuple -- ({node: parent}, {node: label})
        """
        raise NotImplementedError

    @staticmethod
    def prune_above_roots(node_to_parent, forced_roots):
        """
        returns a new node_to_parent where every remaining node is a descendant of the forced_roots
        """

        if forced_roots is None:
            return node_to_parent

        for (node, parent) in node_to_parent.items():
            ancestors = []
            while parent:
                ancestors.append(parent)
                parent = node_to_parent.get(parent)
            if not any(root in ancestors for root in forced_roots):
                node_to_parent.pop(node, None)
        return node_to_parent


class CollabForecastBuilder(HierarchyBuilder):
    """
    Sync the hierarchy from a Collaborative Forecast in SFDC.
    Connects to the ETL service to get the latest state of the User and UserRole datasets.
    """
    ROLE_ID = 'UserRoleId'
    PARENT_ROLE_ID = 'ParentRoleId'
    FCST_USER_ID = 'ForecastUserId'
    IS_ACTIVE = 'IsActive'
    FORECAST_ENABLED = 'ForecastEnabled'
    NAME = 'Name'

    # For testing only.
    local_results = False

    include_role_name = False

    def build_hierarchy(self):
        # Get mappings from role to user, role to parent and role to forecast user.

        if self.local_results:
            role_file = 'roles.json'
            try:
                with open(role_file) as f:
                    roles = json.load(f)
            except IOError:
                with open(role_file, 'w+') as f:
                    roles = self._fetch_latest_roles()
                    json.dump(roles, f)
        else:
            roles = self._fetch_latest_roles()

        if self.local_results:
            user_file = 'users.json'
            try:
                with open(user_file) as f:
                    users = json.load(f)
            except IOError:
                with open(role_file, 'w+') as f:
                    users = self._fetch_latest_users()
                    json.dump(users, f)
        else:
            users = self._fetch_latest_users()

        user_to_role = {x: y[self.ROLE_ID]
                        for (x, y) in users.items() if y[self.ROLE_ID]}
        role_to_parent = {x: y[self.PARENT_ROLE_ID]
                          for (x, y) in roles.items() if y.get(self.PARENT_ROLE_ID)}
        role_to_fcst_user = {x: y[self.FCST_USER_ID]
                             for (x, y) in roles.items() if y.get(self.FCST_USER_ID)}

        labels = {x: y[self.NAME] for x, y in users.items()}
        for role, dtls in roles.items():
            if dtls[self.FCST_USER_ID] in users:
                # We shouldn't only get the ones that are in users (what if they're deleted).
                user_name = users[role_to_fcst_user[role]][self.NAME]
                labels[role] = user_name
                if self.include_role_name:
                    labels[role] += ' (%s)' % (dtls[self.NAME])
                labels[dtls[self.FCST_USER_ID]] = "%s's Own Opportunities" % user_name

        non_forecast_users = {ID for ID, x in users.items()
                              if x[self.FORECAST_ENABLED] == 'false'}
        bad_users = non_forecast_users  # | inactive_users (inactives are included in collab forecasting)
        user_to_role = {user: role for user, role in user_to_role.items()
                        if user not in bad_users}

        node_to_parent = {x: role_to_parent[x]
                          for x in role_to_parent
                          if x in role_to_fcst_user}

        node_to_parent.update({user: role_to_parent[role]
                               for user, role in user_to_role.items()
                               if role in role_to_parent and role_to_parent[role] in role_to_fcst_user
                               })

        node_to_parent.update({user: role
                               for role, user in role_to_fcst_user.items()
                               })

        # Create nodes for FORECAST USERS

        node_to_parent = HierarchyBuilder.prune_above_roots(node_to_parent, self.config.forced_roots)

        roots = set(node_to_parent.values()) - set(node_to_parent)

        node_to_parent.update({root: None for root in roots})

        labels = {x: y for (x, y) in labels.items() if x in node_to_parent}
        node_to_parent = {x: y for (x, y) in node_to_parent.items() if x in labels}

        return node_to_parent, labels

    def _fetch_latest_roles(self, ):

        etl_svc = sec_context.get_microservice_config('etl_data_service')
        if etl_svc:
            etl_shell = sec_context.etl
            needed_fields = [self.PARENT_ROLE_ID, self.FCST_USER_ID, self.NAME]

            record_iterator = etl_shell.uip('UIPIterator',
                                            **{'dataset': 'UserRole',
                                               'fields_requested': needed_fields,
                                               })

        return {rec.ID: {x: rec.getLatest(x, None) for x in needed_fields}
                for rec in record_iterator}

    def _fetch_latest_users(self, ):

        needed_fields = [self.ROLE_ID, self.FORECAST_ENABLED,
                         self.IS_ACTIVE, self.FCST_USER_ID, self.NAME]

        etl_svc = sec_context.get_microservice_config('etl_data_service')
        if etl_svc:
            etl_shell = sec_context.etl
            query_params = {
                'dataset': 'User',
                'fields_requested': needed_fields,
            }
            record_iterator = etl_shell.uip('UIPIterator', **query_params)

        return {rec.ID: {x: rec.getLatest(x, None) for x in needed_fields}
                for rec in record_iterator}


class DrilldownBuilder(HierarchyBuilder):
    """
    base class for drilldown builders
    need to adhere to contract of having build_hierarchy method
    """

    def __init__(self, period, drilldown, deal_count=None, seed=None, drilldown_label=None):
        self.config = HierConfig()
        self.period = period
        self.drilldown = drilldown
        self.drilldown_label = drilldown_label if drilldown_label else self.config.drilldown_labels[drilldown]

        # only used for dummy + test tenant generation
        self.deal_count = deal_count
        self.seed = seed


class FlatBuilder(DrilldownBuilder):
    """
    get a flat hierarchy with a single root node
    """

    def build_hierarchy(self):
        return {'root': None}, {'root': 'Global'}


class NoDrilldownBuilder(DrilldownBuilder):
    def build_hierarchy(self):
        root = '#'.join(['!', '!'])
        not_in_hier = '#'.join(['!', 'not_in_hier'])
        dd_node_to_parent = {root: None,
                             not_in_hier: root}
        dd_labels = {root: 'Root', not_in_hier: 'Not in Hierarchy'}
        timestamp = epoch().as_epoch()  # TODO: cheating

        if self.config.versioned_hierarchy:
            timestamp, _ = get_period_begin_end(self.period)

        node_to_parent, labels = fetch_node_to_parent_mapping_and_labels(timestamp,
                                                                         drilldown=False,
                                                                         period=self.period)

        dd_node_to_parent.update({'#'.join(['!', node]): '#'.join(['!', parent])
        if parent else root
                                  for node, parent in node_to_parent.items()})

        dd_labels.update({'#'.join(['!', node]): label
                          for node, label in labels.items()})

        return dd_node_to_parent, dd_labels


class HierOnlyBuilder(DrilldownBuilder):
    def build_hierarchy(self, service=None):
        root = '#'.join([self.drilldown, '!'])
        not_in_hier = '#'.join([self.drilldown, 'not_in_hier'])
        dd_node_to_parent = {root: None,
                             not_in_hier: root}
        dd_labels = {root: ' '.join([self.drilldown_label, 'Root']), not_in_hier: 'Not in Hierarchy'}
        timestamp = epoch().as_epoch()  # TODO: cheating
        if self.config.versioned_hierarchy:
            timestamp, _ = get_period_begin_end(self.period)

        node_to_parent, labels = fetch_node_to_parent_mapping_and_labels(timestamp,
                                                                         drilldown=False,
                                                                         period=self.period, service=service
                                                                         )

        dd_node_to_parent.update({'#'.join([self.drilldown, node]): '#'.join([self.drilldown, parent])
        if parent else root
                                  for node, parent in node_to_parent.items()})

        dd_labels.update({'#'.join([self.drilldown, node]): label
        if node_to_parent.get(node) is not None or is_lead_service(service) else self.drilldown_label
                          for node, label in labels.items()})

        return dd_node_to_parent, dd_labels


class DealBottomBuilder(DrilldownBuilder):
    def build_hierarchy(self):
        self.root = '##'.join([self.drilldown, '!'])
        not_in_hier = '##'.join([self.drilldown, 'not_in_hier'])
        dd_node_to_parent = {self.root: None,
                             not_in_hier: self.root}
        dd_labels = {self.root: ' '.join([self.drilldown_label, 'Root']), not_in_hier: 'Not in Hierarchy'}
        timestamp = epoch().as_epoch()  # TODO: cheating
        if self.config.versioned_hierarchy:
            timestamp, _ = get_period_begin_end(self.period)
        node_to_parent, labels = fetch_node_to_parent_mapping_and_labels(timestamp,
                                                                         drilldown=False,
                                                                         period=self.period)

        # deal based drilldowns added to bottom of existing hierarchy
        leaves = set(node_to_parent.keys()) - set(node_to_parent.values())

        deal_fields = self.config.drilldown_deal_fields[self.drilldown]
        drilldown_field_values = self.deal_field_values['|'.join(deal_fields)]

        for node, parent in node_to_parent.items():
            dd_node = '#'.join([self.drilldown, '', node])
            if parent:
                dd_node_to_parent[dd_node] = '#'.join([self.drilldown, '', parent])
            else:
                dd_node_to_parent[dd_node] = self.root

            dd_labels[dd_node] = labels[node] if node_to_parent.get(node) is not None else self.drilldown_label

            if node in leaves:
                ntp, ls = self.create_bottom_nodes(self.drilldown, node, drilldown_field_values)
                dd_node_to_parent.update(ntp)
                dd_labels.update(ls)
        return dd_node_to_parent, dd_labels

    def create_bottom_nodes(self, drilldown, node, drilldown_tuples):
        node_to_parent, labels = {}, {}
        for drilldown_tuple in drilldown_tuples.get(node, []):
            parent = '#'.join([drilldown, '', node])
            for drilldown_values in (drilldown_tuple[:i + 1] for i in range(len(drilldown_tuple))):
                child = '#'.join([drilldown, '||'.join(['|'.join([field, val])
                                                        for field, val in drilldown_values if field and val]), node])
                node_to_parent[child] = parent
                labels[child] = drilldown_values[-1][1]
                parent = child
        return node_to_parent, labels

    @cached_property
    def deal_field_values(self):
        deal_fields = self.config.drilldown_deal_fields[self.drilldown]
        if not self.config.dummy_tenant:
            values = sec_context.gbm.api(
                '/gbm/drilldown_fields?period={}&owner_mode=True&drilldown={}'.format(self.period, self.drilldown),
                {'fields_list': [deal_fields]}) or {self.period: {}}
            return values[self.period]
        return get_distinct_deal_field_values(self.period, deal_fields, self.deal_count, self.seed)


class DealTopBuilder(DrilldownBuilder):
    def build_hierarchy(self):
        self.root = '##'.join([self.drilldown, '!'])
        not_in_hier = '##'.join([self.drilldown, 'not_in_hier'])
        dd_node_to_parent = {self.root: None,
                             not_in_hier: self.root}
        dd_labels = {self.root: ' '.join([self.drilldown_label, 'Root']), not_in_hier: 'Not in Hierarchy'}
        node_to_parent, self.labels = self.get_node_to_parent_and_labels()

        roots = {node for node, parent in node_to_parent.items() if not parent}
        self.hier_root = list(roots)[0]
        if len(roots) == 1:
            self.old_root = roots.pop()
            node_to_parent.pop(self.old_root)
            roots = {node for node, parent in node_to_parent.items() if parent == self.old_root}
            self.old_root = '##'.join([self.drilldown, self.old_root])
            dd_node_to_parent[self.old_root] = self.root
            dd_labels[self.old_root] = self.drilldown_label
        else:
            # TODO: this seems suspect, should we allow this from a product perspective?
            # if the hierarchy has multiple root nodes (yikes)
            # insert a parent above those hierarchy roots, that reports the the drilldown root
            self.old_root = '#'.join([self.drilldown, 'root', '!'])
            dd_node_to_parent[self.old_root] = self.root
            dd_labels[self.old_root] = self.drilldown_label

        deal_fields = self.config.drilldown_deal_fields[self.drilldown]
        drilldown_field_values = self.deal_field_values['|'.join(deal_fields)]

        # create new top of tree using deal field values
        tree_top, tree_top_labels = self.create_top_nodes(self.drilldown, drilldown_field_values, roots)
        dd_node_to_parent.update(tree_top)
        dd_labels.update(tree_top_labels)

        for tree_top_leaf in set(tree_top.keys()) - set(tree_top.values()):
            # add existing hierarchy to the bottom of tree top
            tree_field_values = tree_top_leaf.split('#')[-2]

            for node, parent in node_to_parent.items():
                if node in roots:
                    continue
                else:
                    parent = '#'.join([self.drilldown, tree_field_values, parent])

                dd_node = '#'.join([self.drilldown, tree_field_values, node])
                dd_node_to_parent[dd_node] = parent
                dd_labels[dd_node] = self.labels[node]

        return dd_node_to_parent, dd_labels

    def get_node_to_parent_and_labels(self):
        timestamp = epoch().as_epoch()  # TODO: cheating
        if self.config.versioned_hierarchy:
            timestamp, _ = get_period_begin_end(self.period)
        return fetch_node_to_parent_mapping_and_labels(timestamp, drilldown=False, period=self.period)

    def create_top_nodes(self, drilldown, drilldown_tuples, nodes):
        node_to_parent, labels = {}, {}
        for drilldown_tuple in drilldown_tuples:
            for i, drilldown_values in enumerate([drilldown_tuple[:i + 1] for i in range(len(drilldown_tuple))][::-1]):
                parent = '#'.join([drilldown, '||'.join(['|'.join([field, val])
                                                         for field, val in drilldown_values]), self.hier_root])
                if not i:
                    children = {'#'.join([drilldown, '||'.join(['|'.join([field, val])
                                                                for field, val in drilldown_values]), node]): node for
                                node in nodes}
                    node_to_parent.update({child: parent for child in children})
                    labels.update({child: self.labels[node] for child, node in children.items()})
                else:
                    node_to_parent[child] = parent
                    labels[child] = child.split('#')[1].split('|')[-1]
                child = parent
            else:
                node_to_parent[child] = self.old_root
                labels[child] = drilldown_tuple[0][1]
        return node_to_parent, labels

    @cached_property
    def deal_field_values(self):
        deal_fields = self.config.drilldown_deal_fields[self.drilldown]
        period = self.period
        if not self.config.dummy_tenant:
            if PeriodsConfig().predict_period == 'Q':
                period = get_period_and_close_periods(self.period)[0]
            values = sec_context.gbm.api('/gbm/drilldown_fields?period={}'.format(period),
                                         {'fields_list': [deal_fields]}) or {period: {}}
            return values[period]
        return get_distinct_deal_field_values(period, deal_fields, self.deal_count, self.seed)


class DealOnlyBuilder(DealTopBuilder):
    def get_node_to_parent_and_labels(self):
        # for deal only, we'll add only the leaf level of the hierarchy to the drilldown tree
        timestamp = epoch().as_epoch()  # TODO: cheating
        if self.config.versioned_hierarchy:
            timestamp, _ = get_period_begin_end(self.period)
        node_to_parent, labels = fetch_node_to_parent_mapping_and_labels(timestamp, drilldown=False, period=self.period)
        roots = {node for node, parent in node_to_parent.items() if not parent}
        hier_root = list(roots)[0]
        leaves = set(node_to_parent.keys()) - set(node_to_parent.values())
        node_to_parent = {leaf: hier_root for leaf in leaves}
        node_to_parent.update({hier_root: None})
        labels = {leaf: labels[leaf] for leaf in leaves}
        return node_to_parent, labels


class LeadHierarchyBuilder(HierarchyBuilder):
    """
        Sync the hierarchy based on Users and UserRoles in SFDC.
        Connects to the ETL service to get the latest state of the User and UserRole datasets.
        """
    ROLE_ID = 'UserRoleId'
    PARENT_ROLE_ID = 'ParentRoleId'
    IS_ACTIVE = 'IsActive'
    NAME = 'Name'

    def build_hierarchy(self):
        # Get mappings from role to user, role to parent and role to forecast user.

        roles = self._fetch_latest_roles()

        users = self._fetch_latest_users()

        user_to_role = {x: y[self.ROLE_ID]
                        for (x, y) in users.items() if y[self.ROLE_ID]}
        role_to_parent = {x: y[self.PARENT_ROLE_ID]
                          for (x, y) in roles.items() if y.get(self.PARENT_ROLE_ID)}

        labels = {x: y[self.NAME] for x, y in roles.items()}

        for user, dtls in users.items():
            if dtls[self.ROLE_ID] in roles:
                user_name = dtls[self.NAME]
                labels[user] = user_name

        node_to_parent = {x: role_to_parent[x]
                          for x in role_to_parent}

        node_to_parent.update({user: role
                               for user, role in user_to_role.items()
                               })

        node_to_parent = HierarchyBuilder.prune_above_roots(node_to_parent, self.config.forced_roots)

        roots = set(node_to_parent.values()) - set(node_to_parent)

        node_to_parent.update({root: None for root in roots})

        labels = {x: y for (x, y) in labels.items() if x in node_to_parent}
        node_to_parent = {x: y for (x, y) in node_to_parent.items() if x in labels}

        return node_to_parent, labels

    def _fetch_latest_roles(self, ):

        etl_svc = sec_context.get_microservice_config('etl_data_service')
        if etl_svc:
            etl_shell = sec_context.etl
            needed_fields = [self.PARENT_ROLE_ID, self.NAME]

            record_iterator = etl_shell.uip('UIPIterator',
                                            **{'dataset': 'UserRole',
                                               'fields_requested': needed_fields,
                                               })

        return {rec.ID: {x: rec.getLatest(x, None) for x in needed_fields}
                for rec in record_iterator}

    def _fetch_latest_users(self, ):

        needed_fields = [self.ROLE_ID, self.IS_ACTIVE, self.NAME]

        etl_svc = sec_context.get_microservice_config('etl_data_service')
        if etl_svc:
            etl_shell = sec_context.etl
            query_params = {
                'dataset': 'User',
                'fields_requested': needed_fields,
            }
            record_iterator = etl_shell.uip('UIPIterator', **query_params)

        return {rec.ID: {x: rec.getLatest(x, None) for x in needed_fields}
                for rec in record_iterator}


HIERARCHY_BUILDERS = {'collab_fcst': CollabForecastBuilder,
                      'flat_hier': FlatBuilder,
                      'upload': None,  # admin responsible for uploading,
                      'lead_hierarchy': LeadHierarchyBuilder
                      }

DRILLDOWN_BUILDERS = {'no_drilldown': NoDrilldownBuilder,
                      'hier_only': HierOnlyBuilder,
                      'deal_top': DealTopBuilder,
                      'deal_partial_hier': DealTopBuilder,
                      'deal_only': DealOnlyBuilder,
                      'deal_bottom': DealBottomBuilder,
                      }

class HierConfig(BaseConfig):

    config_name = 'hierarchy'

    @cached_property
    def segment_rules_updated(self):
        return self.config.get('segment_rules_updated', False)

    @cached_property
    def dummy_tenant(self):
        """
        tenant is using dummy data, is not hooked up to any gbm/etl
        """
        return self.config.get('dummy_tenant', False)

    @cached_property
    def bootstrapped(self):
        """
        tenant has been bootstrapped and has initial data loaded into app
        """
        return self.config.get('bootstrapped', True)
    
    @cached_property
    def bootstrapped_drilldown(self):
        """
        tenant has been bootstrapped and has drilldown data loaded into app
        """
        return self.config.get('bootstrapped_drilldown', True)

    @cached_property
    def versioned_hierarchy(self):
        """
        hierarchy is versioned
        periods will display data using hierarchy as of that period
        unversioned hierarchies have one version for all of time
        changes to unversioned hierarchies will impact data in previous periods

        Returns:
            bool
        """
        return self.config.get('versioned', False)

    @cached_property
    def logger_draw_tree(self):
        """
        Enable or disable the draw tree function.
        Default: Enabled

        Returns:
            bool
        """
        return self.config.get('logger_draw_tree',True)

    @cached_property
    def hierarchy_builder(self):
        """
        type of hierarchy builder to use to make hierarchy from source data for syncing

        Returns:
            str -- type of hier builder
        """
        # TODO: assert etl connection exists, assert hier builder exists
        return self.config.get('hierarchy_builder')

    @cached_property
    def forced_roots(self):
        """
        hierarchy node to set as root node

        Returns:
            list -- [nodes]
        """
        return self.config.get('forced_roots')

    #
    # Drilldown Configuration
    #
    @cached_property
    def drilldown_builders(self):
        """
        mapping from drilldown to type of drilldown builder to use to make drilldown

        Returns:
            dict -- {drilldown: drilldown builder}
        """
        return {drilldown: dd_dtls['drilldown_builder']
                for drilldown, dd_dtls in self.config.get('drilldowns', {}).items()}

    @cached_property
    def drilldown_labels(self):
        """
        mapping from drilldown to label

        Returns:
            dict -- {drilldown: label}
        """
        return {drilldown: dd_dtls.get('label', drilldown)
                for drilldown, dd_dtls in self.config.get('drilldowns', {}).items()}

    @cached_property
    def drilldown_deal_fields(self):
        """
        mapping from drilldown to deal fields used in drilldown

        Returns:
            dict -- {drilldown: [deal fields]}
        """
        return {drilldown: dd_dtls.get('deal_fields', [])
                for drilldown, dd_dtls in self.config.get('drilldowns', {}).items()}

    @cached_property
    def deal_only_drilldowns(self):
        """
        drilldowns that are only based on deal attributes, no hierarchy included

        Returns:
            set -- {drilldowns}
        """
        return {drilldown for drilldown, dd_dtls in self.config.get('drilldowns', {}).items()
                if dd_dtls['drilldown_builder'] == 'deal_only'}

    @cached_property
    def deal_partial_hier_drilldowns(self):
        """
        drilldowns that are based on deal attributes, and a select number of levels from hierarchy

        Returns:
            dict -- {drilldowns: number of hierarchy levels to keep}
        """
        return {drilldown: dd_dtls['num_levels'] for drilldown, dd_dtls in self.config.get('drilldowns', {}).items()
                if dd_dtls['drilldown_builder'] == 'deal_partial_hier'}

    def validate(self, config):
        dummy_tenant = config.get('dummy_tenant', False)
        hier_builder = config.get('hierarchy_builder')
        good_hier_builder, hier_builder_msg = self._validate_hierarchy_builder(hier_builder, dummy_tenant)
        good_drilldowns, drilldowns_msg = self._validate_drilldowns(config.get('drilldowns', {}), dummy_tenant)
        return good_hier_builder and good_drilldowns, [hier_builder_msg, drilldowns_msg]

    def _validate_hierarchy_builder(self, hierarchy_builder, dummy_tenant):
        if not hierarchy_builder and dummy_tenant:
            return True, ''
        if hierarchy_builder not in HIERARCHY_BUILDERS:
            return False, 'hierarchy builder: {} not in available hierarchy builders: {}'.format(hierarchy_builder, sorted(HIERARCHY_BUILDERS.keys()))
        if hierarchy_builder in ['collab_fcst'] and not sec_context.get_microservice_config('etl_data_service'):
            # TODO: if we ever build other syncers, check them here
            return False, 'not allowed to have syncing hierarchy without an etl service connection'
        return True, ''

    def _validate_drilldowns(self, drilldowns, dummy_tenant):
        gbm_svc = sec_context.get_microservice_config('gbm_service')
        if gbm_svc:
            #TODO: Need discussion: Understanding how domainmodel is working
            from domainmodel.datameta import Dataset
            ds = Dataset.getByNameAndStage('OppDS', full_config=True).get_as_map()
            gbm_dd_config = get_nested(ds, ['models', 'common', 'config', 'viewgen_config'], {})
            uipfields = set(get_nested(ds, ['params', 'general', 'uipfield'], []))

        for dd, dd_dtls in drilldowns.items():
            dd_builder = dd_dtls.get('drilldown_builder')
            if dd_builder not in DRILLDOWN_BUILDERS:
                return False, 'drilldown builder: {} not in available drilldown builders: {}'.format(dd_builder, sorted(DRILLDOWN_BUILDERS.keys()))
            if dd_builder in ['deal_top', 'deal_bottom', 'deal_only', 'deal_partial_hier']:
                deal_fields = dd_dtls.get('deal_fields')
                if not deal_fields:
                    return False, 'no deal fields for deal based drilldown: {}, {}'.format(dd, dd_dtls)
                if not dummy_tenant and not gbm_svc:
                    return False, 'not allowed to have deal based hierarchy without a gbm service'
                if dummy_tenant:
                    continue
                gbm_deal_fields = _get_deal_fields(gbm_dd_config, dd)
                if deal_fields != gbm_deal_fields:
                    return False, 'deal fields in gbm and hier svc config must match. gbm: {}, hier: {}'.format(gbm_deal_fields, deal_fields)
                not_in_uip = set(deal_fields) - uipfields
                if not_in_uip:
                    return False, 'deal fields must all be in uipfields, missing from uip: {}'.format(list(not_in_uip))
            if dd_builder == 'deal_partial_hier':
                if 'num_levels' not in dd_dtls:
                    return False, 'must configure number of hierarchy levels to include for {}'.format(dd)
        return True, ''

def _get_deal_fields(gbm_dd_cfg, dd):
    gbm_node_key = get_nested(gbm_dd_cfg, ['drilldown_config', dd, 'node'], '')
    return get_nested(gbm_dd_cfg, ['node_config', gbm_node_key, 'fields'], [])

def write_hierarchy_to_gbm(config,
                           db=None):
    """
    write hierarchy collection to paired gbm service

    Arguments:
        config {HierConfig} -- config for hierarchy, from HierConfig            ...?

    Keyword Arguments:
        db {pymongo.database.Database} -- instance of tenant_db
                                          (default: {None})
                                          if None, will create one

    Returns:
        bool -- success
    """
    db = db if db else sec_context.tenant_db
    gbm_svc = sec_context.get_microservice_config('gbm_service')
    if not gbm_svc:
        return True

    # TODO: dont compute checksum on the fly
    gbm_checksum = sec_context.gbm.api('/gbm/hier_metadata', None)
    hier_svc_checksum = create_collection_checksum(HIER_COLL, db=db)
    if config.debug:
        logger.info('gbm checksum: %s, app checksum: %s', gbm_checksum, hier_svc_checksum)
    if gbm_checksum == hier_svc_checksum:
        return True

    hier_collection = db[HIER_COLL]
    hier_records = list(hier_collection.find({}, {'_id': 0}))

    logger.info('resyncing hierarchy to gbm, hier_svc: %s, gbm: %s, sample rec:', hier_svc_checksum,
                gbm_checksum,
                try_index(hier_records, 0))

    try:
        sec_context.gbm.api('/gbm/hier_sync', hier_records)
    except Exception as e:
        logger.warning(e.message)
        return False

    return True
