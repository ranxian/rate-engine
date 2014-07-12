import MySQLdb
import os

conn = MySQLdb.connect('ratedev-server', 'root', 'testing', 'rate3')
c = conn.cursor()

f = open("/Users/yyk/Desktop/enroll_result.txt", "r")
i = 1
for line in f.readlines():
    (u,s) = line.strip().split(' ')
    if s=="failed":
        c.execute('select file from sample where uuid = unhex(replace("%s", "-", ""))' % (u, ))
        f = c.fetchone()[0]
        f = os.path.join('./RATE_ROOT', 'samples', f)
        print "enroll.exe %s %d.t" % (f, i)
        i = i+1

#        if os.path.exists(f):
#            print f, os.stat(f).st_size
#        else:
#            print f, "missing"
