from django.contrib.auth.models import AbstractUser
from django.db.models.fields import AutoField


class MyAutoField(AutoField):

    def to_python(self, value):
        return value

    def value_to_string(self, obj):
        return obj


class User(AbstractUser):
    """Target for AUTH_USER_MODEL only.

    Users live in Mongo and are represented at runtime by GnanaUser, which the
    aviso package builds. Nothing is ever written to this table.
    """

    myid = MyAutoField(primary_key=True)

    def save(self, *args, **kwargs):
        raise Exception("Trying to save data to django user model which we don't support!!")
