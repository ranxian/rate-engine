import sys
import shutil
import os

def benchmark2bxx(benchmark_dir):
    try:
        src_file = os.path.join(benchmark_dir, 'benchmark.txt')
        dst_file = os.path.join(benchmark_dir, 'benchmark_bxx.txt')
        tmp_file = dst_file+".unsorted"
        table_file = os.path.join(benchmark_dir, 'uuid_table.txt')

        uuids = {}
        srcf = open(src_file, 'r')
        tmpf = open(tmp_file, 'w')
        tablef = open(table_file, 'w')
        print "bxxing"
        while True:
            l = srcf.readline()
            if len(l)==0:
                break
            l = l.strip()
            f1 = srcf.readline().strip()
            f2 = srcf.readline().strip()
            fs = (f1, f2)
            (u1, u2, mt) = l.split(' ')
            us = (u1, u2)
            for u in us:
                if u not in uuids:
                    uuids[u] = hex(len(uuids)+1)[2:]
                    print>>tablef, uuids[u], u, fs[us.index(u)]
            h1 = uuids[u1]
            h2 = uuids[u2]
            if h1>h2:
                h1, h2 = h2, h1
            print>>tmpf, h1, h2, mt

        srcf.close()
        tablef.close()
        tmpf.close()

        print "sorting"
        os.system('sort %s -o %s' % (tmp_file, dst_file+".tmp"))
        shutil.move(dst_file+".tmp", dst_file)
        os.remove(tmp_file)
        #os.remove(src_file)

        return True
    except Exception, e:
        print e
        return False

if __name__=='__main__':

    USAGE = """
    %s benchmark_dir
    it converts benchmark.txt into benchmark_bxx.txt and uuid_table.txt under benchmark_dir
    """

    if len(sys.argv)!=2:
        print USAGE % sys.argv[0]

    benchmark_dir = sys.argv[1]
    benchmark2bxx(benchmark_dir)
