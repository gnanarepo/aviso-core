from utils.common import cached_property


class CoolerSplitterService:
    '''
    '''
    res_fields = {
        'eACV', 'forecast', 'bs_forecast',
        'active_amount', 'won_amount', 'lost_amount',
        'existing_pipe_active_amount', 'existing_pipe_won_amount', 'existing_pipe_lost_amount',
        'new_pipe_active_amount', 'new_pipe_won_amount', 'new_pipe_lost_amount',
    }

    dummy_info = {'num': res_fields, 'str': set()}

    def __init__(self, config):
        self.primary_config = config['primary']
        self.other_configs = {i: cfg for i, cfg in enumerate(config.get('other', []))}

    @cached_property
    def ratio_fields(self):
        return {
            'primary': self.primary_config['ratio_field'],
            'other': {idx: other_config['ratio_field'] for idx, other_config in self.other_configs.items()}
        }

    @cached_property
    def split_fields(self):
        output = {}
        output['primary'] = {
            'num': set(self.primary_config.get('num_fields', [])),
            'str': set(self.primary_config.get('str_fields', [])),
        }
        output['other'] = {}
        for idx, other_config in self.other_configs.items():
            output['other'][idx] = {
                'num': set(other_config.get('num_fields', [])),
                'str': set(other_config.get('str_fields', [])),
            }
        return output

    @property
    def unborn_amount_fields(self):
        # for the new v2 unborn
        output = set()
        for split in self.splitters.values():
            output |= {split.unborn_amount_fld}
        return output

    @cached_property
    def split_info(self):
        output = {'num': set(), 'str': set()}
        output['num'] |= self.split_fields['primary']['num'] | self.res_fields
        output['str'] |= self.split_fields['primary']['str']
        for idx, other_fields in self.split_fields['other'].items():
            output['num'] |= other_fields['num']
            output['str'] |= other_fields['str']
        return output

    @cached_property
    def all_fields(self):
        output = self.split_fields['primary']['num'] | self.split_fields['primary']['str'] | self.res_fields
        for idx, other in self.split_fields['other'].items():
            output |= other['num'] | other['str']
        return output

    @cached_property
    def unborn_fields(self):
        return {self.ratio_fields['primary']} | set(self.ratio_fields['other'].values())

    def create_splits(self, record):
        output = {fld: {} for fld in self.all_fields if fld in record}
        # first we handle the primary
        p_ratios = record[self.ratio_fields['primary']]
        # if you don't have ratios, you get unmapped, tough luck
        if not p_ratios or p_ratios == 'N/A':
            p_ratios = {'unmapped': 1.}
        split_ids = set(p_ratios.keys())
        prim_num_fields = set(output) & (self.split_fields['primary']['num'] | self.res_fields)
        prim_str_fields = set(output) & self.split_fields['primary']['str']
        for split_id, ratio in p_ratios.items():
            for field in prim_num_fields:
                output[field][split_id] = self.try_num_val(record[field], ratio)
            for field in prim_str_fields:
                output[field][split_id] = self.try_str_val(record[field], split_id)
                if field + '_ratios' not in output:
                    output[field + '_ratios'] = {}
                output[field + '_ratios'][split_id] = ratio
            # for field in set(output) & self.ignore_fields:
            #    output[field][split_id] = record[field]

        # other configs are assumed to not have to deal with stuff
        for idx, other_fld in self.ratio_fields['other'].items():
            o_ratios = record[other_fld]
            if not o_ratios:
                o_ratios = {'unmapped': 1.}
            o_num_fields = set(output) & self.split_fields['other'][idx]['num']
            o_str_fields = set(output) & self.split_fields['other'][idx]['str']
            for split_id, ratio in o_ratios.items():
                for field in o_num_fields:
                    output[field][split_id] = self.try_num_val(record[field], ratio)
                for field in o_str_fields:
                    output[field][split_id] = self.try_num_ratio(record[field], split_id)
                    if field + '_ratios' not in output:
                        output[field + '_ratios'] = {}
                    output[field + '_ratios'][split_id] = ratio
        return output, split_ids

    def try_num_val(self, val, ratio):
        try:
            return val * ratio
        except:
            return None

    def try_str_val(self, val, split_id):
        if isinstance(val, str):
            return val
        try:
            return val[split_id]
        except:
            return 'N/A'

    def create_unborn_splits(self, record, amt_field):
        raw_amount = record[amt_field]
        amount = 0 if raw_amount == 'N/A' else raw_amount
        raw_ratios = record[self.ratio_fields['primary']]
        ratios = raw_ratios if raw_ratios else {'dummy': 1}
        output = {}
        for split_id, ratio in ratios.items():
            try:
                output[split_id] = amount * ratio
            except TypeError:
                output[split_id] = 0
        return output, set(ratios.keys())

    @classmethod
    def dummy_splits(cls, record):
        return {fld: {'dummy': record[fld]} for fld in cls.res_fields if fld in record}, None

    @staticmethod
    def dummy_unborn_splits(record, fld):
        raw_amount = record[fld]
        amount = 0 if raw_amount == 'N/A' else raw_amount
        return {'dummy': amount}, None
