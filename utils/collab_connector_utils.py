import pymongo

from domainmodel.app import User


def get_dict_of_all_fm_users(db_to_use=None, consider_admin=True):
    db_to_use = db_to_use if db_to_use is not None else User.get_db()
    all_users_list = list(db_to_use.findDocuments(
        User.getCollectionName(), {"object.is_disabled": False}, sort=[('object.username', pymongo.ASCENDING)]))
    all_users_dict = {}
    for u in all_users_list:
        data = u['object']
        roles = data['roles'].get("user", [])
        if not consider_admin:
            admin_user = data['roles'].get("administrator", None)
            if admin_user:
                continue
        consider = True
        for role in roles:
            if role[0] == 'results' and role[1] == '*':
                consider = False
                break
        if consider:
            all_users_dict[data['username']] = {
                "name": data["name"],
                'email': data['email'],
                'roles': data['roles'],
                'userId': data.get('user_id'),
                'user_role': data.get('user_role')
            }
    return all_users_dict
