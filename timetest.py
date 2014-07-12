import os 
from timeit import Timer

def doMatch():
	os.system("match.exe 1.t 2.t")
def doEnroll():
	os.system("enroll.exe x.bmp x.t")
def doMatchR():
	os.system("rate_run.exe 1000 5248000 match.exe 1.t 2.t")
def doEnrollR():
	os.system("rate_run.exe 1000 5248000 enroll.exe x.bmp x.t")

te = Timer("doEnroll()", "from __main__ import doEnroll")
te = te.timeit(100)
tm = Timer("doMatch()","from __main__ import doMatch")
tm = tm.timeit(100)
ter = Timer("doEnrollR()", "from __main__ import doEnrollR")
ter = ter.timeit(100)
tmr = Timer("doMatchR()","from __main__ import doMatchR")
tmr = tmr.timeit(100)

print 
print 
print "100 enroll time: ", te
print "100 match time: ", tm
print "100 enroll time with rate_run.exe: ", ter
print "100 match time with rate_run.exe: ", tmr

