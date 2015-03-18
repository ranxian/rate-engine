import sys
import shutil
import os
sys.path.append(os.path.dirname(__file__))
from producer import RateProducer
import traceback

if __name__=='__main__':
    usage = """
    %s host benchmark_uuid result_dir algorithm_uuid timelimit memlimit
    host is the host where queue server is on
    result_dir can be absolute or releative path
    timelimit in ms
    memlimit in byte
    """ % (sys.argv[0])
    if len(sys.argv)!=7:
        print usage
        exit()

    try:
        benchmark_dir = 'benchmarks' + '/' +  sys.argv[2]

        benchmark_bxx_file = "/".join((benchmark_dir, "benchmark_bxx.txt"))
        uuid_table_file = "/".join((benchmark_dir, "uuid_table.txt"))

        if not os.path.exists(benchmark_bxx_file) or not os.path.exists(uuid_table_file):
            from benchmark2bxx import benchmark2bxx
            if not benchmark2bxx(benchmark_dir):
                print "failed to benchmark2bxx"

        p = RateProducer(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])
        p.solve()
        print 'ok'
    except Exception, e:
        print e
        traceback.print_exc()
