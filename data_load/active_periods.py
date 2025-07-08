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
        import requests
        url = "https://app.aviso.com/prd_svc/active_periods"
        payload = {}
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'priority': 'u=1, i',
            'referer': 'https://app.aviso.com/welcome/home/newhome/2025Q3/2025Q3/CRO%23!/CRO%23!',
            'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
            'Cookie': '_gcl_au=1.1.1931291717.1746098952; _rdt_uuid=1746098952804.1288fce1-b32a-4d9c-86fd-82207e0e27a7; _ga=GA1.2.1731304971.1746098952; _hjSessionUser_762446=eyJpZCI6IjNmMWNkM2Y2LWRiZjgtNTg2Ny1hY2QyLTIyZjRjZTQyNmM3YiIsImNyZWF0ZWQiOjE3NDYwOTg5NTM4OTcsImV4aXN0aW5nIjpmYWxzZX0=; __adroll_fpc=84bc19461e53efb7eead5840b1c199fb-1746098953906; _fbp=fb.1.1746098953925.553367864677358319; _clck=fmhwbp%7C2%7Cfvj%7C0%7C1947; messagesUtk=3eddf8351955472f8c459988bbc522d2; _lc2_fpi=bb000863780a--01jt5sqj21xr0vbb2pyr4y7gfj; _li_ss=CgA; _ga_5MF7CCWNXW=GS1.1.1746098952.1.1.1746100078.60.0.1249082096; amp_c0849c_aviso.com=X7kGseh3ZQ86Ze8TdZozRC.d2FxYXMuYWhtZWQ=..1iq7pkjvs.1iq7pnn8a.0.0.0; intercom-device-id-f6m6fegc=f6e1bf7d-2ffc-48a7-9cbb-823d7c95add2; x-g-device-id=2cbe7a23-ad1a-4f67-8f6d-3c9915f34a25; AMP_MKTG_c0849cbf4d=JTdCJTdE; x-g-no-refresh=1; csrftoken=l54OJWJ2ubLNXkJfBa67xx7Xnv0d5lLe; sessionid=iwmmllaovrx89eusskevusid4il3ywyk; x-g-tenant=nextech.com; AMP_c0849cbf4d=JTdCJTIyZGV2aWNlSWQlMjIlM0ElMjJYN2tHc2VoM1pRODZaZThUZFpvelJDJTIyJTJDJTIydXNlcklkJTIyJTNBJTIyd2FxYXMuYWhtZWQlNDBhdmlzby5jb20lMjIlMkMlMjJzZXNzaW9uSWQlMjIlM0ExNzUxOTUyMTcxNTU5JTJDJTIyb3B0T3V0JTIyJTNBZmFsc2UlMkMlMjJsYXN0RXZlbnRUaW1lJTIyJTNBMTc1MTk1MjE3MTU3NiUyQyUyMmxhc3RFdmVudElkJTIyJTNBMzU3JTdE; mp_47d9b92825c2521d50b853cd41fc0f30_mixpanel=%7B%22distinct_id%22%3A%22%24device%3A8756941b-51eb-4e9a-8239-bfb22edbbbc5%22%2C%22%24device_id%22%3A%228756941b-51eb-4e9a-8239-bfb22edbbbc5%22%2C%22TENANT%22%3A%22nextech.com%22%2C%22USERTYPE%22%3A%22Internal%22%2C%22%24initial_referrer%22%3A%22https%3A%2F%2Fadministrative.app.aviso.com%2Faccount%2Fvalidateuser%2F324ea6fc-62f5-49f3-84f9-4e4f98c6f919--d2FxYXMuYWhtZWRAYWRtaW5pc3RyYXRpdmUuZG9tYWlu%22%2C%22%24initial_referring_domain%22%3A%22administrative.app.aviso.com%22%2C%22__mps%22%3A%7B%7D%2C%22__mpso%22%3A%7B%22%24initial_referrer%22%3A%22https%3A%2F%2Fadministrative.app.aviso.com%2Faccount%2Fvalidateuser%2F324ea6fc-62f5-49f3-84f9-4e4f98c6f919--d2FxYXMuYWhtZWRAYWRtaW5pc3RyYXRpdmUuZG9tYWlu%22%2C%22%24initial_referring_domain%22%3A%22administrative.app.aviso.com%22%7D%2C%22__mpus%22%3A%7B%7D%2C%22__mpa%22%3A%7B%7D%2C%22__mpu%22%3A%7B%7D%2C%22__mpr%22%3A%5B%5D%2C%22__mpap%22%3A%5B%5D%2C%22%24search_engine%22%3A%22google%22%7D; sessionid=iwmmllaovrx89eusskevusid4il3ywyk'
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        return response.json()

    def get_current_quarter_boq_eoq(self):
        active_periods = self.fetch_actvie_periods()
        for year, year_data in active_periods.items():
            if not isinstance(year_data, dict) or 'quarters' not in year_data:
                continue
            for qtr, qtr_data in year_data['quarters'].items():
                if qtr_data.get('relative_period') == 'c':
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
