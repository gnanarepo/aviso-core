from Crypto.Cipher import AES
import base64
import hashlib
from aviso.settings import sec_context
from bson import BSON

def encrypt(key, message):
    cipher = AES.new(key, mode=AES.MODE_CFB, IV="0123456789012345")
    return base64.b64encode(cipher.encrypt(message))

def encrypt_stream(key, message):
    cipher = AES.new(key, mode=AES.MODE_CFB, IV="0123456789012345")
    for x in message:
        yield base64.b64encode(cipher.encrypt(x))

def decrypt(key, message):
    cipher = AES.new(key, mode=AES.MODE_CFB, IV="0123456789012345")
    return cipher.decrypt(base64.b64decode(message))

def decrypt_stream(key, message):
    cipher = AES.new(key, mode=AES.MODE_CFB, IV="0123456789012345")
    for x in message:
        yield cipher.decrypt(base64.b64decode(x))


class Wallet:
    def __init__(self, wallet):

        # Find the master key
        try:
            f = open('/etc/master_key','r')
            self.master_key = f.readline()
            f.close()
        except:
            self.master_key = 'h6nTho9Mi4i7e77BX7miJ8DFrqiOxfzf'

        self.wallet = wallet

        # Load keys
        keys = {}
        try:
            f = open(wallet,'r')
            for kline in f:
                (n,k)=kline.split('=',1)
                if k:
                    keys[n]=k
            f.close()
        except IOError as e:
            if not e.errno == 2:
                pass
        self.keys = keys

    def add(self, key, name):
        self.keys[name]= encrypt(self.master_key, key)

    def __getitem__(self, name):
        return decrypt(self.master_key, self.keys[name])

    def save(self):
        # Save all the keys
        f = open(self.wallet,'w')
        for n,k in self.keys.items():
            f.write("=".join([n,k]))
            f.write('\n')
        f.close()

def getfilemd5(file_path, block_size=2**24):
        md5 = hashlib.md5()
        with open (file_path,'r') as f:
            while True:
                data = f.read(block_size)
                if not data:
                    break
                md5.update(data)
        return md5.hexdigest()

def setpath(dic, atr, val):
    if len(atr) == 1:
        dic[atr[0]] = val
    else:
        setpath(dic.setdefault(atr[0], {}), atr[1:], val)


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


def encrypt_dict_leaves(d):
    if isinstance(d, dict):
        return dict((k, encrypt_dict_leaves(v)) for k,v in d.iteritems())
    elif isinstance(d, (list,set,tuple)):
        return [encrypt_dict_leaves(v1) for v1 in d]
    else:
        return sec_context.encrypt(d)
