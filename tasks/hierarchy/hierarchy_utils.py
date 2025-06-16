import logging

from aviso.settings import sec_context

logger = logging.getLogger("gnana." + __name__)

def get_user_permissions(user, app_section):
    roles = user.roles['user']
    if isinstance(roles, dict):
        return roles.get(app_section, {}).keys()
    elif isinstance(roles, list):
        return next(x for x in roles if x[0] == app_section)[1:]
