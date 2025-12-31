import sys
import os
import site
import fileinput
import itertools
import types
import zipimport

# ---- Fix Python2 izip ----
if not hasattr(itertools, "izip"):
    itertools.izip = zip

# ---- Pre-create a dummy `core` module so `import core` never fails ----
sys.modules["core"] = types.ModuleType("core")

if not hasattr(zipimport.zipimporter, "_orig_find_spec"):
    zipimport.zipimporter._orig_find_spec = zipimport.zipimporter.find_spec

def patched_find_spec(self, fullname, path=None, target=None):
    """
    Patched version of zipimporter.find_spec to accept optional 'path' and 'target'.
    """
    try:
        # Try calling original find_spec with both path and target if supported
        return self._orig_find_spec(fullname, target)
    except TypeError:
        # fallback for old SDKs
        return self._orig_find_spec(fullname)

# Apply patch
zipimport.zipimporter.find_spec = patched_find_spec

def fix_pyschema():
    """
    Fix pyschema Python2 incompatibilities:
    1. itertools.izip
    2. absolute import: `import core`
    """

    # ---- Fix 1: itertools.izip ----
    if not hasattr(itertools, "izip"):
        itertools.izip = zip

    # ---- Fix 2: make `import core` resolve ----
    # We cannot import pyschema.core until izip exists
    import pyschema.core
    sys.modules["core"] = pyschema.core

    # ---- Optional: patch site-packages for safety ----
    try:
        site_packages = site.getsitepackages()[0]
        pyschema_dir = os.path.join(site_packages, "pyschema")
        types_path = os.path.join(pyschema_dir, "types.py")

        if os.path.exists(types_path):
            with fileinput.FileInput(types_path, inplace=True) as f:
                for line in f:
                    if line.strip() == "import core":
                        print("from . import core")
                    else:
                        print(line, end="")

        init_path = os.path.join(pyschema_dir, "__init__.py")
        if os.path.exists(init_path):
            with open(init_path, "r") as f:
                content = f.read()

            if "Schema = PySchema" not in content:
                with open(init_path, "a") as f:
                    f.write("\nfrom .core import PySchema\nSchema = PySchema\n")

        print("Successfully patched pyschema")

    except Exception as e:
        print(f"Warning: pyschema patch skipped: {e}")


fix_pyschema()


import threading
import django

period = "2026Q1"
user_name = "waqas.ahmed"
tenant_name = "wiz_qa.io"


def initialize_system():
    """Initialize Django and security context"""

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "aviso.settings")
    django.setup()

    from aviso.framework import SecurityContext

    # Create security context
    security_context = SecurityContext()


    class TenantDetails:
        def __init__(self, name, period):
            self.name = name
            self.period = period
            self._configs = {
                "micro_app": {
                    "deal_config": {},
                    "hier_config": {},
                    "periods_config": {},
                }
            }

        def get_config(self, category, config_name):
            return self._configs.get(category, {}).get(config_name, {})

        def set_config(self, category, config_name, value):
            self._configs.setdefault(category, {})[config_name] = value

        def get_all_config(self):
            return self._configs

    tenant_details = TenantDetails(tenant_name, period)

    if not hasattr(security_context, "thread_local"):
        security_context.thread_local = threading.local()

    security_context.thread_local.tenant_used_for_db = tenant_name
    security_context.thread_local.tenant_db = tenant_name
    security_context.thread_local.tenant_details = tenant_details
    security_context.thread_local.details = tenant_details

    security_context.set_context(
        user_name=user_name,
        tenant_name=tenant_name,
        login_tenant_name=tenant_name,
        login_user_name=user_name,
        switch_type="tenant",
        csv_version_info={},
    )

    from aviso import settings
    settings.sec_context = security_context

    return security_context


if __name__ == "__main__":
    try:
        security_context = initialize_system()

        from config.deal_config import DealConfig

        db = security_context.get_tenant_db()
        print("DB:", db.name)
        print("Collections:", db.list_collection_names())

        from data_load.data_load import *

        stack = 'preprod'
        gbm_stack='gbm-qa'
        etl_stack='etl-qa'
        pod = 'dev'
        id_list = []
        dl = DataLoad(
            id_list,
            tenant_name,
            stack,
            gbm_stack,
            pod,
            etl_stack,
            period,
            run_type='chipotle',
            from_timestamp=0,
            changed_fields_only=False)
        basic_result = dl.get_basic_results()
        print(len(basic_result))

        # deal_config = DealConfig()
        # print("Deal Config:", deal_config.config.get("update_new_collection"))

    except Exception as e:
        print(f"Error during initialization: {e}")
        raise