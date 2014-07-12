import sys
import shutil
import os

def matchresult2bxx(benchmark_dir, result_dir):
    try:
        uuid_table_file = os.path.join(benchmark_dir, 'uuid_table.txt')
        match_result_file = os.path.join(result_dir, 'match_result.txt')
        tmp_bxx_result_file = os.path.join(result_dir, 'match_result_bxx.unsorted.txt')
        bxx_result_file = os.path.join(result_dir, 'match_result_bxx.txt')

        uuid_table_f = open(uuid_table_file, 'r')
        uuid_bxx_table = {}
        for l in uuid_table_f.readlines():
            (bxx, u, f) = l.strip().split(' ')
            uuid_bxx_table[u] = bxx
        uuid_table_f.close()

        match_result_f = file(match_result_file, 'r')
        tmp_bxx_result_f = file(tmp_bxx_result_file, 'w')
        while True:
            l = match_result_f.readline()
            if len(l)==0:
                break
            columns = l.strip().split(' ')
            columns[0] = uuid_bxx_table[columns[0]]
            columns[1] = uuid_bxx_table[columns[1]]
            print>>tmp_bxx_result_f, " ".join(columns)
        match_result_f.close()
        tmp_bxx_result_f.close()

        os.system('sort %s -o %s' % (tmp_bxx_result_file, bxx_result_file))
        os.remove(tmp_bxx_result_file)
        os.remove(match_result_file)

        return True
    except Exception, e:
        print e
        return False

if __name__=='__main__':

    USAGE = """
    %s benchmark_dir result_dir
    it converts match_result.txt into match_result_bxx.txt and sort it under benchmark_dir
    """

    if len(sys.argv)!=3:
        print USAGE % sys.argv[0]
        exit()

    benchmark_dir = sys.argv[1]
    result_dir = sys.argv[2]
    matchresult2bxx(benchmark_dir, result_dir)
