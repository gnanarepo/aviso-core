import functools
import hashlib
import logging
import os
import pprint
import time
from subprocess import PIPE, Popen

import boto
from aviso.settings import BACKUP_CONFIG, ISPROD, sec_context
from boto.s3.key import Key

from utils import GnanaError

logger = logging.getLogger('gnana.%s' % __name__)

def filemd5(filename, block_size=2**20):
    f = open(filename)
    md5 = hashlib.md5()
    while True:
        data = f.read(block_size)
        if not data:
            break
        md5.update(data)
    f.close()
    return md5.hexdigest()

def progress_callback(up_down, bytes_transmitted, total_bytes):
    logger.info("Completed %2f.0%% of the %s" % (float(bytes_transmitted * 1000/total_bytes)/10, up_down))

def bucket_acc(directory_name, filename, key_name, autodelete=False, download=False, upload=False,
               retries = 3):
    s3Connect = boto.connect_s3()

    bucket = s3Connect.get_bucket('aviso-tenant-copy', validate=False)

    # EV Nov 17, 2014: This logic is not correct.  You want exactly one of
    # download, upload or autodelete to be true.  It will require more
    # combinations
    if not (download or upload or autodelete):
        raise Exception('Either upload or download must be True')
    if (download and upload and autodelete):
        raise Exception('Both upload or download can not be True')

    if upload:
        k = Key(bucket)
        k.key = key_name + "/"+filename
        if k.key:
            expanded_path = os.path.expanduser(directory_name+"/" + filename)
            k.set_contents_from_filename(
                expanded_path,
                cb=functools.partial(progress_callback, 'upload'),
                num_cb=21)

    if download:
        for i in range(retries):
            total_size = 0
            k = bucket.get_key(key_name + "/" + filename)
            if os.path.exists(directory_name + "/" + filename):
                remote_checksum = k.etag[1:-1]
                local_file_md5 = filemd5(os.path.join(directory_name, filename))
                if local_file_md5 == remote_checksum:
                    return True
                else:
                    os.remove(os.path.join(directory_name, filename))
            expanded_directoryname = os.path.expanduser(directory_name + "/" + filename)
            try:
                logger.info("Attempt : %d " % (i + 1))
                total_size = k.size
                k.get_contents_to_filename(
                    expanded_directoryname,
                    cb=functools.partial(progress_callback, 'download'),
                    num_cb=21)
                break
            except Exception as e:
                logger.info("Restore failed due to %s" % (e))
                logger.info("Bucket Size: %d " % (total_size))
                wait_time = (i + 1) * 30
                logger.info("Waiting for %d seconds before retrying" % (wait_time))
                time.sleep(wait_time)
                if i == 2:
                    raise e
                pass

    if autodelete:
            bucket.delete_key(key_name+"/"+filename)
    return True

upload_to_s3 = functools.partial(bucket_acc, upload=True)
download_from_s3 = functools.partial(bucket_acc, download=True)
delete_from_s3 = functools.partial(bucket_acc, autodelete=True)
