# config/__init__.py
# Avoid top-level imports that may touch Django models

# Only import "safe" modules
from .base_config import BaseConfig, BadConfigurationError
from .deal_config import DealConfig
from .fm_config import DEFAULT_ROLE, FMConfig
from .periods_config import PeriodsConfig

# For modules that may touch Django models, do lazy imports via functions
def get_hier_config():
    from .hier_config import HierConfig
    return HierConfig

def get_hierarchy_builders():
    from .hier_config import HIERARCHY_BUILDERS
    return HIERARCHY_BUILDERS

def get_drilldown_builders():
    from .hier_config import DRILLDOWN_BUILDERS
    return DRILLDOWN_BUILDERS

def get_write_hierarchy_to_gbm():
    from .hier_config import write_hierarchy_to_gbm
    return write_hierarchy_to_gbm

# Backwards compatible direct symbol used by tests and older code
try:
    write_hierarchy_to_gbm = get_write_hierarchy_to_gbm()
except Exception:
    # If the import triggers environment-specific failures during certain test runs,
    # fall back to a dummy function that raises a clear error when used.
    def write_hierarchy_to_gbm(*args, **kwargs):
        raise RuntimeError("write_hierarchy_to_gbm not available in this environment")


# __all__ can still expose names
__all__ = [
    "BaseConfig",
    "BadConfigurationError",
    "PeriodsConfig",
    "FMConfig",
    "DealConfig",
    "DEFAULT_ROLE",
    # lazy-loaded objects
    "get_hier_config",
    "get_hierarchy_builders",
    "get_drilldown_builders",
    "get_write_hierarchy_to_gbm",
]
