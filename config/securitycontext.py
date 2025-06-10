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
                elif line.strip() == 'import core':
                    print('from . import core')
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
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')
    django.setup()

    from aviso.framework import SecurityContext
    from aviso.domainmodel.tenant import Tenant

    # Create security context
    security_context = SecurityContext()

    # Settings
    period = '2026Q1'
    user_name = 'waqas.ahmed'
    tenant_name = 'netapp.com'

    # Initialize thread local storage
    if not hasattr(security_context, 'thread_local'):
        security_context.thread_local = threading.local()

    # Create tenant
    tenant = Tenant()
    tenant.name = tenant_name
    tenant.period = period
    tenant.flags = {
        'molecule_status': {
            'rtfm': {}
        }
    }

    # Set thread local attributes
    security_context.thread_local.tenant_used_for_db = tenant_name
    security_context.thread_local.tenant_db = f"{tenant_name}_db"
    security_context.thread_local.tenant_details = tenant
    security_context.tenant_details = tenant

    # Set context
    security_context.set_context(
        user_name=user_name,
        tenant_name=tenant_name,
        login_tenant_name=tenant_name,
        login_user_name=user_name,
        switch_type='tenant',
        csv_version_info={}
    )

    return security_context

if __name__ == '__main__':
    try:
        fix_pyschema()

        security_context = initialize_system()

        # Step 4: Print verification info
        print("Security Context Name:", security_context.name)
        print("tenant db Name:", security_context.tenant_db)
        db = security_context.get_tenant_db()
        print("GET Tenant DB:", db.deals.count_documents({}))
        try:
            ...
            # flags = security_context.details.get_flag('molecule_status', 'rtfm', {})
            #print("Flags:", flags)
        except AttributeError:
            print("Flags: {}")

    except Exception as e:
        print(f"Error during initialization: {str(e)}")
        raise