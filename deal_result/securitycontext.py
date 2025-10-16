import threading
import sys
import os
import django
import site
import fileinput
from six.moves import zip_longest as izip


def fix_pyschema():
    """Fix pyschema package imports and add Schema to __init__.py"""
    sys.modules['itertools'].izip = izip

    site_packages = site.getsitepackages()[0]
    pyschema_dir = os.path.join(site_packages, 'pyschema')
    types_path = os.path.join(pyschema_dir, 'types.py')

    try:
        with fileinput.FileInput(types_path, inplace=True) as file:
            for line in file:
                if line.strip() == 'from core import ParseError, Field, auto_store, PySchema':
                    print('from .core import ParseError, Field, auto_store, PySchema')
                elif line.strip() == 'from core import auto_store':
                    print('from .core import auto_store')
                else:
                    print(line, end='')

        init_path = os.path.join(pyschema_dir, '__init__.py')
        with open(init_path, 'r') as f:
            content = f.read()

        if 'Schema = PySchema' not in content:
            with open(init_path, 'a') as f:
                f.write('\nfrom .core import PySchema\nSchema = PySchema\n')

        print("Successfully patched pyschema")
    except Exception as e:
        print(f"Warning: Could not patch pyschema: {str(e)}")

def initialize_system():
    """Initialize Django and security context"""
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'aviso.settings')
    django.setup()

    from aviso.framework import SecurityContext
    from aviso.domainmodel.tenant import Tenant

    from aviso.settings import sec_context
    security_context = sec_context


    # Settings
    period = '2025Q1'
    user_name = 'waqas.ahmed'
    tenant_name = 'wiz_qa.io'
    # tenant_name = 'cisco_qa_cache_preprod'

    # Create tenant with required methods
    class TenantDetails:
        def __init__(self, name, period):
            self.name = name
            self.period = period
            self._configs = {
                'micro_app': {
                    'deal_config': {},
                    'hier_config': {},
                    'periods_config': {}
                }
            }

        def get_config(self, category, config_name):
            return self._configs.get(category, {}).get(config_name, {})

        def set_config(self, category, config_name, value):
            if category not in self._configs:
                self._configs[category] = {}
            self._configs[category][config_name] = value

        def get_all_config(self):
            return self._configs

    # Create tenant details instance
    tenant_details = TenantDetails(tenant_name, period)

    # Initialize thread local storage
    if not hasattr(security_context, 'thread_local'):
        security_context.thread_local = threading.local()

    # Set thread local attributes
    security_context.thread_local.tenant_used_for_db = tenant_name
    security_context.thread_local.tenant_db = f"{tenant_name}"
    security_context.thread_local.tenant_details = tenant_details
    security_context.thread_local.details = tenant_details

    # Set context
    security_context.set_context(
        user_name=user_name,
        tenant_name=tenant_name,
        login_tenant_name=tenant_name,
        login_user_name=user_name,
        switch_type='tenant',
        csv_version_info={}
    )

    # Update the global sec_context


    return security_context

if __name__ == '__main__':
    try:
        fix_pyschema()
        security_context = initialize_system()

        # Import DealConfig after Django is initialized
        from config.deal_config import DealConfig
        from deal_result.deals_results import get_deals_results
        db = security_context.get_tenant_db()
        print(db)
        deals=get_deals_results(
            tenant_name='wiz_qa.io',
            stack='preprod',
            gbm_stack='gbm-qa',
            etl_stack='etl-qa',
            pod='dev',
            periods=['2025Q1','2025Q2','2025Q3','2025Q4'],
            timestamps=[],
            get_results_from_as_of=0,
            fields=[],
            node=None,
            force_uip_and_hierarchy=False,
            include_uip=True,
            allow_live=False,
            return_files_list=False
        )
        print(deals)


    except Exception as e:
        print(f"Error during initialization: {str(e)}")
        raise



