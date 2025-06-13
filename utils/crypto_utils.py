from Crypto.Cipher import AES
import base64
from aviso.settings import sec_context
from bson import BSON

def encrypt(key, message):
    cipher = AES.new(key, mode=AES.MODE_CFB, IV="0123456789012345")
    return base64.b64encode(cipher.encrypt(message))

def decrypt(key, message):
    cipher = AES.new(key, mode=AES.MODE_CFB, IV="0123456789012345")
    return cipher.decrypt(base64.b64decode(message))


def extract_index(index_list, attributes, query_fields=[]):
    new_obj = {}
    #Query fields are added to the indexed list to populate in the database
    if query_fields:
        indexed_fields = index_list.keys()
        for field in query_fields:
            if field not in indexed_fields:
                index_list[field] = {}
    for key in index_list.keys():
        if 'index_spec' in index_list[key]:
            # Get all the first items from index spec, which will be the
            # field.  Remove 'object.' using [7:]
            field_list = [i[0][7:] for i in index_list[key]['index_spec']]
        elif 'key' in index_list[key]:
            field_list = [i[0][7:] for i in index_list[key]['key']]
        else:
            field_list = [key]
        for k in field_list:
            key_segments = k.split('.')
            parent = attributes
            for key_segment in key_segments:
                if key_segment in parent:
                    parent = parent.get(key_segment)
                else:
                    break
            else:
                value_position=new_obj
                for key_segment in key_segments[0:-1]:
                    if key_segment not in value_position:
                        value_position[key_segment] = {}
                    value_position = value_position[key_segment]
                value_position[key_segments[-1]] = sec_context.encrypt(parent)
    return new_obj


def encrypt_record(index, record, query_fields=[], postgres=False, cls=None):
    new_key = sec_context.aes_key
    new_check = sum(bytearray(str(new_key))) # check regarding check sum entry

    encdata = sec_context.encrypt(BSON.encode({'dynamic_fields':record['object'].get('dynamic_fields', {})} if postgres else record['object']),
                                  cls=cls)
#     print 'encrypt data  ' +  encdata + '\n'

    result = record.copy()
    result['_encdata'] = encdata
    result['object'] = extract_index(index, record['object'], query_fields)
    result['_check'] = new_check
    return result
