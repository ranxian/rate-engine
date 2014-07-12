import memcache
import json
import sys
from producer import formMatchKey

USAGE="""
%s algorithm_version_uuid match_result.txt
TODO: match_result.txt is deprecated, use match_result_bxx.txt
"""

hosts = ['localhost:11211', ]

if __name__=='__main__':
    if len(sys.argv)!=3:
        print USAGE % sys.argv[0]
        exit()

    conn = memcache.Client(hosts, debug=0)
    algorithm_version_uuid = sys.argv[1]
    mf = sys.argv[2]
    mf = open(mf, 'r')
    i = 0
    while True:
        i = i+1
        if i%10000==0:
            print i
        l = mf.readline()
        if len(l)==0:
            break
        if l.find('ok')>=0:
            (u1, u2, mt, result, score) = l.strip().split(' ')
            cache_key = formMatchKey(algorithm_version_uuid, u1, u2)
            cache_value = ['ok', mt, score]
            conn.set(cache_key, json.dumps(cache_value))

