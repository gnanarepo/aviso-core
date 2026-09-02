from aviso.framework.authentication import MongoBackend, SAMLBackend


def _with_django_meta(user):
    """Restore the attribute django.contrib.auth.login() relies on.

    login() serialises the session key as ``user._meta.pk.value_to_string(user)``.
    GnanaUser answers ``pk`` but the packaged aviso wheel dropped the
    ``_meta = self`` assignment the original carried, so put it back.
    """
    if user is not None and not hasattr(user, '_meta'):
        user._meta = user
    return user


class SessionMongoBackend(MongoBackend):
    """MongoBackend with a Django >= 1.11 compatible signature.

    django.contrib.auth.authenticate() binds ``request`` as the first argument
    and silently skips any backend whose signature does not accept it.
    """

    def authenticate(self, request=None, username=None, password=None):
        return _with_django_meta(
            super().authenticate(username=username, password=password))

    def get_user(self, user_key):
        return _with_django_meta(super().get_user(user_key))


class SessionSAMLBackend(SAMLBackend):

    def authenticate(self, request=None, username=None, password=None):
        return _with_django_meta(
            super().authenticate(username=username, password=password))

    def get_user(self, user_key):
        return _with_django_meta(super().get_user(user_key))
