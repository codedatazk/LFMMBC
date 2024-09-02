import hashlib
import pymysql
import time

def hash_caculate(s):
    md5 = hashlib.md5()
    md5.update(str(s).encode('utf-8'))
    return md5.hexdigest()

def Link_1(n):
    id_1 = n - 5
    pcode_first = hash_caculate(id_1)
    return pcode_first


def Link_2(n):
    id_2 = n - 10
    pcode_second = hash_caculate(id_2)
    return pcode_second