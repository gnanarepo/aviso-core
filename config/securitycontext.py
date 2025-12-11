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
    # Try to use the new Django settings module, fallback to old if needed
    if 'DJANGO_SETTINGS_MODULE' not in os.environ:
        os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'aviso_core.settings')
    
    try:
        django.setup()
    except Exception as e:
        # If Django is already set up, continue
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(f"Django setup issue (may already be initialized): {e}")

    # Try to import SecurityContext - create a stub if not available
    try:
        from aviso.framework import SecurityContext
    except ImportError:
        # Create a basic SecurityContext stub for compatibility
        class SecurityContext:
            def __init__(self):
                self.thread_local = None
                self.login_user_name = None
                self.tenant_db = None
                self.etl = None
                self.gbm = None
            
            def set_context(self, **kwargs):
                self.login_user_name = kwargs.get('login_user_name')
                # Set other attributes as needed
            
            def get_tenant_db(self):
                return self.tenant_db
    
    # Try to import Tenant - create a stub if not available
    try:
        from aviso.domainmodel.tenant import Tenant
    except ImportError:
        Tenant = None

    # Create security context
    security_context = SecurityContext()

    # Settings
    period = '2026Q1'
    user_name = 'waqas.ahmed'
    tenant_name = 'cisco_qa.com'
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
    from aviso import settings
    settings.sec_context = security_context

    return security_context

if __name__ == '__main__':
    try:
        fix_pyschema()
        security_context = initialize_system()

        # Import DealConfig after Django is initialized
        from config.deal_config import DealConfig

        db = security_context.get_tenant_db()
        print(db)
        print("GET Tenant DB:", db.deals.estimated_document_count())
        print("DB name: ", db.name)
        print("DB deals name: ", db.deals.name)
        print("DB deals count: ", db.deals.find_one())
        print("GET Tenant DB collections:", db.list_collection_names())
        # Print the database and collection names: print(db.name, db.deals.name)
        # Use db.deals.count_documents({}) for an accurate count.

        deal_config = DealConfig()
        print("Deal Config:", deal_config.config.get('update_new_collection'))

    except Exception as e:
        print(f"Error during initialization: {str(e)}")
        raise