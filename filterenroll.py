import sys
import os

USAGE="""
USAGE: %s [benchmarkfile] [enrollfile]
"""

def filterEnroll(benchmarkf, enrollf):
    print "generating %s for %s" % (enrollf, benchmarkf)
    bf = benchmarkf
    ef = enrollf
    if os.path.exists(ef):
        print "%s already exists, remove it first"
        exit()
    bf = open(bf, 'r')
    ef = open(ef, 'w')
    enroll_uuids = set()
    count_of_matches = 0

    while True:
        a = bf.readline().strip()
        if len(a)==0:
            break
        count_of_matches += 1
        if count_of_matches%100000==0:
            print "filterenroll: %d matches, %d enrolls" % (count_of_matches, len(enroll_uuids))
        b = bf.readline().strip()
        c = bf.readline().strip()
        match = [a,b,c]
        us = match[0].strip().split(' ')[:2]
        for u in us:
            if u not in enroll_uuids:
                f = "/".join(('samples', match[us.index(u)+1].strip()))
                print>>ef, "%s %s" % (u, f)
                enroll_uuids.add(u)
    bf.close()
    ef.close()

if __name__=='__main__':
    if len(sys.argv)!=3:
        print USAGE % sys.argv[0]
        exit()

    filterEnroll(sys.argv[1], sys.argv[2])



