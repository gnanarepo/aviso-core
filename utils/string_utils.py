import string
import random


def first15(s):
    if s and len(s)>15:
        return s[0:15]
    else:
        return s

def random_string(size=8, chars=None):
    if chars is None:
        chars = string.ascii_letters + string.digits
    return ''.join(random.choice(chars) for x in range(size))
