from contextlib import contextmanager

from aviso.settings import sec_context


# @contextmanager
# def get_shell(name):
#     shell = getattr(sec_context, name)
#     shell.async = True
#     yield shell
#     shell.async = False

@contextmanager
def get_shell(name):
    shell = getattr(sec_context, name)
    setattr(shell, 'async', True)
    try:
        yield shell
    finally:
        setattr(shell, 'async', False)



class PeriodResolver:

    def fetch_actvie_periods(self):
        # with get_shell('forecast_app') as fm_shell:
        #     active_periods = fm_shell.api("/prd_svc/active_periods", None)
        # return active_periods
        return {u'period_level': u'year', u'2023': {u'end': 1704095999999, u'begin': 1672560000000, u'relative_period': u'h', u'has_forecast': False, u'quarters': {u'2023Q4': {u'end': 1704095999999, u'begin': 1696143600000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1713432259605}, u'2023Q2': {u'end': 1688194799999, u'begin': 1680332400000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1689412302514}, u'2023Q3': {u'end': 1696143599999, u'begin': 1688194800000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1697418326013}, u'2023Q1': {u'end': 1680332399999, u'begin': 1672560000000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1681974732817}}}, u'2024': {u'end': 1735718399999, u'begin': 1704096000000, u'relative_period': u'h', u'has_forecast': False, u'quarters': {u'2024Q1': {u'end': 1711954799999, u'begin': 1704096000000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1718972762706}, u'2024Q3': {u'end': 1727765999999, u'begin': 1719817200000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1727765999999}, u'2024Q2': {u'end': 1719817199999, u'begin': 1711954800000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1721095451985}, u'2024Q4': {u'end': 1735718399999, u'begin': 1727766000000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1735718399999}}}, u'2025': {u'end': 1767254399999, u'begin': 1735718400000, u'relative_period': u'c', u'has_forecast': False, u'quarters': {u'2025Q4': {u'end': 1767254399999, u'begin': 1759302000000, u'relative_period': u'f', u'has_forecast': True, u'latest_eoq_time': 1721980324025}, u'2025Q1': {u'end': 1743490799999, u'begin': 1735718400000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1743490799999}, u'2025Q2': {u'end': 1751353199999, u'begin': 1743490800000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1751353199999}, u'2025Q3': {u'end': 1759301999999, u'begin': 1751353200000, u'relative_period': u'c', u'has_forecast': True, u'latest_eoq_time': 1721980324025}}}, u'2026': {u'end': 1798790399999, u'begin': 1767254400000, u'relative_period': u'f', u'has_forecast': False, u'quarters': {u'2026Q4': {u'end': 1798790399999, u'begin': 1790838000000, u'relative_period': u'f', u'has_forecast': True, u'latest_eoq_time': 1721980324025}, u'2026Q3': {u'end': 1790837999999, u'begin': 1782889200000, u'relative_period': u'f', u'has_forecast': True, u'latest_eoq_time': 1721980324025}, u'2026Q2': {u'end': 1782889199999, u'begin': 1775026800000, u'relative_period': u'f', u'has_forecast': True, u'latest_eoq_time': 1721980324025}, u'2026Q1': {u'end': 1775026799999, u'begin': 1767254400000, u'relative_period': u'f', u'has_forecast': True, u'latest_eoq_time': 1721980324025}}}, u'2020': {u'end': 1609487999999, u'begin': 1577865600000, u'relative_period': u'h', u'has_forecast': False, u'quarters': {u'2020Q4': {u'end': 1609487999999, u'begin': 1601535600000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1609487999999}, u'2020Q1': {u'end': 1585724399999, u'begin': 1577865600000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1585724399999}, u'2020Q3': {u'end': 1601535599999, u'begin': 1593586800000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1601535599999}, u'2020Q2': {u'end': 1593586799999, u'begin': 1585724400000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1593586799999}}}, u'2018': {u'end': 1546329599999, u'begin': 1514793600000, u'relative_period': u'h', u'has_forecast': False, u'quarters': {u'2018Q1': {u'end': 1522565999999, u'begin': 1514793600000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1522565999999}, u'2018Q2': {u'end': 1530428399999, u'begin': 1522566000000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1530428399999}, u'2018Q3': {u'end': 1538377199999, u'begin': 1530428400000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1538377199999}, u'2018Q4': {u'end': 1546329599999, u'begin': 1538377200000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1546329599999}}}, u'2022': {u'end': 1672559999999, u'begin': 1641024000000, u'relative_period': u'h', u'has_forecast': False, u'quarters': {u'2022Q3': {u'end': 1664607599999, u'begin': 1656658800000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1667395821880}, u'2022Q2': {u'end': 1656658799999, u'begin': 1648796400000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1662161082341}, u'2022Q1': {u'end': 1648796399999, u'begin': 1641024000000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1662354527443}, u'2022Q4': {u'end': 1672559999999, u'begin': 1664607600000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1674056611887}}}, u'latest_refresh_date': 1721979066978, u'2019': {u'end': 1577865599999, u'begin': 1546329600000, u'relative_period': u'h', u'has_forecast': False, u'quarters': {u'2019Q4': {u'end': 1577865599999, u'begin': 1569913200000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1577865599999}, u'2019Q1': {u'end': 1554101999999, u'begin': 1546329600000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1554101999999}, u'2019Q3': {u'end': 1569913199999, u'begin': 1561964400000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1569913199999}, u'2019Q2': {u'end': 1561964399999, u'begin': 1554102000000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1561964399999}}}, u'2021': {u'end': 1641023999999, u'begin': 1609488000000, u'relative_period': u'h', u'has_forecast': False, u'quarters': {u'2021Q1': {u'end': 1617260399999, u'begin': 1609488000000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1617260399999}, u'2021Q2': {u'end': 1625122799999, u'begin': 1617260400000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1625122799999}, u'2021Q3': {u'end': 1633071599999, u'begin': 1625122800000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1633071599999}, u'2021Q4': {u'end': 1641023999999, u'begin': 1633071600000, u'relative_period': u'h', u'has_forecast': True, u'latest_eoq_time': 1657185041749}}}}

    def get_current_quarter_boq_eoq(self, period):
        active_periods = self.fetch_actvie_periods()
        for year, year_data in active_periods.items():
            if not isinstance(year_data, dict) or 'quarters' not in year_data:
                continue
            for qtr, qtr_data in year_data['quarters'].items():
                if qtr == period and qtr_data.get('relative_period') == 'c':
                    boq = qtr_data['begin']
                    eoq = qtr_data['end']
                    return boq, eoq
        return None, None

    def get_boq_eoq(self, run_type, period):
        if run_type == 'chipotle':
            return None, None

        active_periods = self.fetch_actvie_periods()
        for year, year_data in active_periods.items():
            if not isinstance(year_data, dict) or 'quarters' not in year_data:
                continue
            for qtr, qtr_data in year_data['quarters'].items():
                if qtr == period and qtr_data.get('relative_period') == ('c' if run_type == 'current' else 'h'):
                    boq = qtr_data['begin']
                    eoq = qtr_data['end']
                    return boq, eoq
        return None, None
