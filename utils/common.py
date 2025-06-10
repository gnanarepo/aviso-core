import json

import netaddr
from aviso.framework.views import GnanaView
from django.http import HttpResponse


class weekday:
    __slots__ = ["weekday", "n"]

    def __init__(self, weekday, n=None):
        self.weekday = weekday
        self.n = n

    def __call__(self, n):
        if n == self.n:
            return self
        else:
            return self.__class__(self.weekday, n)

    def __eq__(self, other):
        try:
            if self.weekday != other.weekday or self.n != other.n:
                return False
        except AttributeError:
            return False
        return True

    def __hash__(self):
        return hash((
          self.weekday,
          self.n,
        ))

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        s = ("MO", "TU", "WE", "TH", "FR", "SA", "SU")[self.weekday]
        if not self.n:
            return s
        else:
            return "%s(%+d)" % (s, self.n)


class cached_property:
    """
    Decorator that converts a method with a single self argument into a
    property cached on the instance.
    """
    def __init__(self, func):
        self.func = func

    def __get__(self, instance, type=None):
        if instance is None:
            return self
        res = instance.__dict__[self.func.__name__] = self.func(instance)
        return res

def ip_match(ip, valid_ip_list):
    ip = ip.strip()
    ip = netaddr.IPAddress(ip)
    for n in valid_ip_list:
        if '/' in n:
            nw = netaddr.IPNetwork(n)
        else:
            nw = netaddr.IPNetwork(n+'/32')
        if ip in nw:
            return n
    return None

class MicroAppView(GnanaView):
    """
    base class for all micro app views
    """
    validators = []

    @cached_property
    def config(self):
        raise NotImplementedError

    def dispatch(self, request, *args, **kwargs):
        if request.method == 'OPTIONS':
            # If the request method is OPTIONS, return an empty response
            return HttpResponse()
        self.debug = request.GET.get('debug')
        if request.GET.get('help'):
            return HttpResponse(json.dumps({'help': self.__doc__}), content_type='application/json')
        for validator in self.validators:
            response = validator(request, self.config)
            if isinstance(response, HttpResponse):
                return response
        return super(MicroAppView, self).dispatch(request, *args, **kwargs)
