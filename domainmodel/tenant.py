import datetime
import threading
import uuid
from aviso.settings import event_context, sec_context
from pymongo import DESCENDING

from domainmodel import Model
from utils import date_utils
from utils.date_utils import EpochClass


class TenantEvents(Model):
    collection_name = "tenant_events"
    version = 1.0
    encrypted = False
    index_list = {
        "created_time": {"direction": DESCENDING},
        "expires": {'expireAfterSeconds': 10},
        "color": {},
        'username': {}
    }

    def __init__(self, attrs=None):
        self.username = None
        self.message = None
        self.created_time = None
        self.end_time = None
        self.color = None
        self.parent_id = None
        self.event_id = None
        self.exception_value = None
        self.params = None
        self.expires = date_utils.now() + datetime.timedelta(90)
        super(TenantEvents, self).__init__(attrs)

    def encode(self, attrs):
        attrs['username'] = self.username
        attrs['message'] = self.message
        attrs['created_time'] = self.created_time
        attrs['color'] = self.color
        attrs['params'] = self.params
        attrs['end_time'] = self.end_time
        attrs['parent_id'] = self.parent_id
        attrs['event_id'] = self.event_id
        attrs['exception_value'] = self.exception_value
        attrs['expires'] = self.expires
        return attrs

    def decode(self, attrs):
        self.username = attrs['username']
        self.message = attrs['message']
        self.created_time = attrs['created_time']
        self.color = attrs['color']
        self.params = attrs.get('params', None)
        self.end_time = attrs.get('end_time', None)
        self.parent_id = attrs['parent_id']
        self.event_id = attrs['event_id']
        self.exception_value = attrs.get('exception_value', None)
        self.expires = attrs['expires']

    @classmethod
    def create_tenant_log(cls, message, color, params, start_time, end_time, parent_id, event_id):
        log_message = cls()
        log_message.message = message
        log_message.color = color
        log_message.params = params
        log_message.created_time = start_time
        log_message.end_time = end_time
        log_message.parent_id = parent_id
        log_message.event_id = event_id
        log_message.username = sec_context.user_name + "@" + sec_context.login_tenant_name
        log_message.save()
        return log_message

    @classmethod
    def update_tenant_log(cls, event_id, end_time, exception_value):
        cursor = cls.getByFieldValue('event_id', event_id)
        cursor.end_time = end_time
        cursor.exception_value = exception_value
        cursor.save(id_field='event_id')

    @classmethod
    def get_records(cls, color=None, username=None, str_msg=None, end_time=None, records=100, sub_events=False):
        criteria = {}
        if color:
            criteria.update({'object.color': {'$in': color}})
        if str_msg:
            regfind = r'('+str_msg+'){1}'
            criteria.update({'object.message': {'$regex': regfind}})
        if username:
            criteria.update({'object.username': username})
        if end_time == 'in_process':
            criteria.update({'object.end_time': None})
        if type(end_time) is int:
            criteria.update({'object.end_time': {'$lt': end_time}})
        if not sub_events:
            criteria.update({'object.parent_id': None})
        records = int(records)
        db_to_use = cls.get_db()
        cursor = db_to_use.findDocuments(
                name=cls.getCollectionName(),
                criteria=criteria,
                sort=[('object.created_time', -1)],
        ).limit(records)
        for x in cursor:
            obj = cls(x)
            yield {'username': obj.username,
                   'color': obj.color,
                   'time': obj.created_time,
                   'message': obj.message,
                   'end_time': obj.end_time,
                   'parent_id': obj.parent_id,
                   'event_id': obj.event_id,
                   'params': obj.params
                   }

    @classmethod
    def get_hierarchy(cls, event_id=None):
        if not event_id:
            return
        return TenantEvents().get_event_tree(TenantEvents().get_root(event_id))

    def get_root(self, event_id):
        if not self.get_parent(event_id):
            return event_id
        else:
            return self.get_root(self.get_parent(event_id))

    def get_event_tree(self, current_id):
        if not current_id:
            return
        else:
            dic = {}
            cursor = self.getAllByFieldValue('event_id', current_id)
            for obj in cursor:
                dic = {'username': obj.username,
                       'color': obj.color,
                       'time': obj.created_time,
                       'message': obj.message,
                       'end_time': obj.end_time,
                       'parent_id': obj.parent_id,
                       'event_id': obj.event_id,
                       'params': obj.params,
                       'children': []
                       }
            child_cursor = self.getAllByFieldValue('parent_id', current_id)
            lst = list(child_cursor)
            child_id = None
            if lst:
                for i in lst:
                    child_id = i.event_id
                    dic['children'].append(self.get_event_tree(child_id))
            return dic

    def get_parent(self, child_id):
        rec = self.getByFieldValue('event_id', child_id)
        parent = rec.parent_id
        return parent


class TenantEventContext:
    event_stack = threading.local()

    def __init__(self, message, color, params):
        self.message = message
        self.color = color
        self.params = params
        self.start_time = None
        self.end_time = None
        self.event_id = str(uuid.uuid4())
        self.parent = None
        self.model_obj = None
        self.exception_value = None

    def __enter__(self):
        self.start_time = EpochClass().as_epoch()
        try:
            self.parent = self.event_stack.stack[-1]  # for sub-events just append in the list with their id
            self.event_stack.stack.append(self.event_id)
        except (IndexError, AttributeError):
            parent_eventid = event_context.event_id
            if parent_eventid:
                self.parent = parent_eventid
            else:
                self.parent = None
            self.event_stack.stack = [self.event_id]  # gives AttributeError and starts the list with parent id
        event_context.set_event_context(self.event_id)
        self.model_obj = TenantEvents.create_tenant_log(self.message, self.color, self.params, self.start_time,
                                                        self.end_time, self.parent, self.event_id)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.event_stack.stack = self.event_stack.stack[0:-1]
        if exc_type:
            self.exception_value = "Type: " + str(exc_type) + ", Value: " + str(exc_value)
        if self.model_obj:
            self.model_obj.end_time = EpochClass().as_epoch()
            self.model_obj.exception_value = self.exception_value
            self.model_obj.save()

def tenant_events(message, color):
    def tenant_event_decorator(fn):
        def new_func(*args, **kwargs):
            with TenantEventContext(message, color, kwargs):
                response = fn(*args, **kwargs)
            return response
        return new_func
    return tenant_event_decorator