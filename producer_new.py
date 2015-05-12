# coding:utf8
# RATE-engine 评测任务生产者

import sys
import time
import pika
import os
import json
import pickle
import ConfigParser
from uuid import uuid4
import threading

config = ConfigParser.ConfigParser()
config.readfp(open('%s/producer.conf' % os.path.dirname(os.path.realpath(__file__)), 'r'))

ENROLL_BLOCK_SIZE = config.getint('rate-server', 'ENROLL_BLOCK_SIZE')
MATCH_BLOCK_SIZE = config.getint('rate-server', 'MATCH_BLOCK_SIZE')
RATE_ROOT = config.get('rate-server', 'RATE_ROOT')
MAX_WORKING_ENROLL_SUBTASKS = config.getint('rate-server', 'MAX_WORKING_ENROLL_SUBTASKS')
MAX_WORKING_MATCH_SUBTASKS = config.getint('rate-server', 'MAX_WORKING_MATCH_SUBTASKS')

ENROLL_PROGRESS_PART = 0.3

class Producer:
    def __init__(self, buuid, auuid, task_uuid, timelimit, memlimit):
        benchmark_dir = "/".join((RATE_ROOT, 'benchmarks', buuid))
        algorithm_dir = "/".join(('algorithms', auuid))
        result_dir = "/".join(('tasks', task_uuid))

        self.buuid = buuid
        self.auuid = auuid
        self.task_uuid = task_uuid
        self.result_dir = result_dir
        self.timelimit = timelimit
        self.memlimit = memlimit
        self.finished = False
        self.host = 'localhost'

        self.benchmark_file_path = "/".join((benchmark_dir, 'benchmark_bxx.txt'))
        self.uuid_table_file_path = "/".join((benchmark_dir, 'uuid_table.txt'))
        self.log_file_path = "/".join((result_dir, 'log.json'))
        self.enroll_result_file_path = "/".join((result_dir, 'enroll_result.txt'))
        self.match_result_file_path = "/".join((result_dir, 'match_result_bxx.txt'))
        self.template_dir = "/".join((result_dir, "templates"))

        self.uuid_bxx_table = {}
        self.bxx_uuid_table = {}
        self.enrollEXE = "/".join((algorithm_dir, 'enroll.exe'))
        self.matchEXE = "/".join((algorithm_dir, 'match.exe'))

        self.enroll_state = {}
        self.match_state = {}
        self.enroll_failed_uuids = {}
        self.enroll_submitted = 0
        self.enroll_finished = 0
        self.enroll_failed = 0
        self.submitting_enroll = False
        self.match_submitted = 0
        self.match_finished = 0
        self.match_failed = 0
        self.submitting_match = False

        self.enroll_lock = threading.Lock() # should be locked whether submitting or receiving result
        self.match_lock = threading.Lock()
        self.heart_beat_lock = threading.Lock()

    def solve(self):
        self.prepare()
        self.doEnroll()
        self.doMatch()

    # 记录任务中间状态
    def dump_log(self):
        information = {
            "task_uuid": self.uuid
        }
        with open(self.log_file_path, 'w') as f:
            f.write(json.dumps(information))

    # 读取任务中间状态
    def load_log(self):
        with open(self.log_file_path, 'r') as f:
            information = json.load(f)
            self.uuid = information["task_uuid"]

    def make_heart_beat(self):
        while True:
            with self.heart_beat_lock:
              self.conn.process_data_events()
            time.sleep(10)
            if self.finished:
              break

    def genSubtask(self, tinytasks, taskType):
        subtask = {
            'timelimit'     : self.timelimit,
            'memlimit'      : self.memlimit,
            'tinytasks'     : tinytasks,
            'task_uuid'     : self.task_uuid,
            'producer_uuid' : self.uuid,
            'enrollEXE'     : self.enrollEXE,
            'matchEXE'      : self.matchEXE,
            'type'          : taskType,
        }
        return subtask

    def submit(self, subtask):
        self.heart_beat_lock.acquire()
        if subtask == None:
            return
        for fpath in subtask['files']:
            fpath = "/".join((RATE_ROOT, fpath))
            if not os.path.exists(fpath):
                raise Exception("file does not exists: %s" % fpath)
        self.ch.basic_publish(exchange='', routing_key=self.job_qname, body=pickle.dumps(subtask))
        self.heart_beat_lock.release()

    def submitEnroll(self, enroll_block, block_no):
        subtask = self.genSubtask(enroll_block, 'enroll')
        files = []
        files.append(self.enrollEXE)
        for task in enroll_block:
            files.append(task['file'])
        subtask['files'] = files
        subtask["block_no"] = block_no
        self.submit(subtask)

    def submitMatch(self, match_block, block_no):
        subtask = self.genSubtask(match_block, 'match')
        files = []
        files.append(self.enrollEXE)
        for i in match_block:
            files.append(i['file1'])
            files.append(i['file2'])
        subtask['files'] = files
        subtask['block_no'] = block_no
        self.submit(subtask)


    def enrollCallBack(self, ch, method, properties, body):
        with self.enroll_lock:
            result = pickle.loads(body)
            for rawResult in result['results']:
                uuid_ = rawResult['uuid']
                print>>self.enroll_result_file, "%s %s" % (uuid_, rawResult['result'])
                self.enroll_result_file.flush()
                if rawResult['result']=='failed':
                    self.enroll_failed += 1
                    self.enroll_failed_uuids.append(rawResult['uuid'])

            self.enroll_state[result['block_no']] = True
            self.dump_log()
            ch.basic_ack(delivery_tag=method.delivery_tag)

            print "enroll result [%s] finished/failed/total [%d/%d/%d]" % \
                (result['block_no'], self.enroll_finished, self.enroll_failed, self.enroll_submitted)

            if (not self.submitting_enroll) and self.enroll_finished == self.enroll_submitted:
                ch.stop_consuming()

    def waitForEnrollResults(self):
        print '[ENROLL] waiting for enroll results'
        conn = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        ch = conn.channel()
        self.enroll_result_ch = ch
        ch.queue_declare(queue=self.enroll_result_qname, durable=False, exclusive=False, auto_delete=False)
        ch.basic_consume(self.enrollCallBack, queue=self.enroll_result_qname)
        ch.start_consuming()
        ch.queue_delete(queue=self.enroll_result_qname)

    def waitForMatchResults(self):
        print '[MATCH] waiting for match results'
        conn = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        ch = conn.channel()
        self.match_result_ch = ch
        ch.queue_declare(queue=self.match_result_qname, durable=False, exclusive=False, auto_delete=False)
        
        ch.basic_consume(self.matchCallBack, queue=self.match_result_qname)
        ch.start_consuming()
        ch.queue_delete(queue=self.match_result_qname)

        self.match_result_file.close()

    def doEnroll(self):
        print '[ENROLL] begin'
        enroll_block = []
        block_no = 0
        self.submitting_enroll = True
        with self.enroll_lock:
            enroll_result_thread = threading.Thread(target=self.waitForEnrollResults)
            enroll_result_thread.daemon = True
            enroll_result_thread.start()

        with open(self.uuid_table_file_path, "r") as f:
            while True:
                line = f.readline()
                if len(line) == 0:
                    break
                line = line.rstrip("\n")
                (bxx, uuid, filepath) = line.split(" ")
                t = {'uuid':uuid, 'file': "samples" + "/" + filepath}
                enroll_block.append(t)

                if len(enroll_block) == ENROLL_BLOCK_SIZE:
                    with self.enroll_lock:
                        self.submitEnroll(enroll_block, block_no)
                        block_no += 1
                        self.enroll_submitted += 1
                        enroll_block = []

        with self.enroll_lock:
            if len(enroll_block) != 0:
                self.submitEnroll(enroll_block, block_no)
                block_no += 1
                self.enroll_submitted += 1

        self.submitting_enroll = False

    def doMatch(self):
        print '[MATCH] begin'
        self.submitting_match = True
        match_block = []
        block_no = 0
        i = 0
        benchmarkf = open(self.benchmark_file_path, 'r')
        with self.match_lock:
            match_result_thread = threading.Thread(target=self.waitForMatchResults)
            match_result_thread.daemon = True
            match_result_thread.start()
        wait = False
        while True:
            if wait == True:
                time.sleep(5)
                wait = False
            with self.match_lock:
                if self.match_submitted - self.match_finished >= MAX_WORKING_MATCH_SUBTASKS:
                    print "%d match queue full, wait for 5 sec" % (self.match_submitted - self.match_finished)
                    wait = True
                    continue
                i += 1

                line = benchmarkf.readline() # 11 22 I
                if len(line) == 0:
                    break

                if i % 100000 == 0:
                    print "%d matches proceeded" % (i,)

                (bxx1, bxx2, gOrI) = line.strip().split(' ')[:3]

                u1 = self.bxx_uuid_table[bxx1]
                u2 = self.bxx_uuid_table[bxx2]

                if u1 in self.enroll_failed_uuids or u2 in self.enroll_failed_uuids:
                    continue

                f1 = '%s/%s.t' % (self.template_dir, u1)
                f2 = '%s/%s.t' % (self.template_dir, u2)
                t = { 'uuid1':u1, 'uuid2':u2, 'file1':f1, 'file2':f2, 'match_type':gOrI }
                match_block.append(t)
                self.match_submitted += 1
                if len(match_block) == MATCH_BLOCK_SIZE:
                    self.submitMatch(match_block, block_no)
                    block_no += 1
                    self.match_submitted += 1
                    match_block = []
                    if block_no % 10 == 0:
                        print "[%d*%d] matches has been submitted" % (self.match_submitted, MATCH_BLOCK_SIZE)

        with self.match_lock:
            if len(match_block)!=0:
                self.submitMatch(match_block, block_no)
                block_no += 1
                self.match_submitted += 1
                match_block = []
            self.submitting_match = False
        benchmarkf.close()

        print "[MATCH] %d matches" % self.match_submitted
        print "[MATCH] all matches submitted, waiting for all results"
        print "[MATCH] match finished, failed %d" % self.match_failed
        with self.heart_beat_lock:
            self.all_finished = True

    def prepare(self):
        print '[PREPARE] begin'

        if os.path.exists(self.log_file_path):
            print '[PREPARE] restore task from log'
            self.load_log()
            self.enroll_result_file = open(self.enroll_result_file_path, 'a')
            self.match_result_file = open(self.match_result_file_path, 'a')
        else:
            if not os.path.isdir(self.result_dir):
                os.makedirs(self.result_dir)
            if not os.path.exists(self.template_dir):
                os.makedirs(self.template_dir)

            self.uuid = uuid4().__str__()
            open(self.match_result_file_path, 'w').close()
            open(self.enroll_result_file_path, 'w').close()
            self.enroll_result_file = open(self.enroll_result_file_path, 'a')
            self.match_result_file = open(self.match_result_file_path, 'a')

            self.dump_log()

        # uuid_table
        with open(self.uuid_table_file_path, 'r') as f:
            while True:
                line = f.readline()
                if line == '':
                    break
                key, uuid, path = line.split()
                self.uuid_bxx_table[uuid] = key
                self.bxx_uuid_table[key] = uuid

        self.enroll_result_qname = 'results-enroll-%s' % (self.uuid,)
        self.match_result_qname = 'results-match-%s' % (self.uuid,)
        self.job_qname = 'jobs-%s' % (self.uuid)
        self.job_qname = 'jobs'

        self.conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.ch = self.conn.channel()
        self.ch.queue_declare(queue=self.job_qname, durable=False, exclusive=False, auto_delete=False)

        hbt = threading.Thread(target=self.make_heart_beat)
        hbt.daemon = True
        hbt.start()
        print '[PREPARE] start heart beat thread'
        print "[PREPARE] finished"


if __name__=='__main__':
    usage = """Usage:
    python %s benchmark_uuid algorithm_uuid result_dir timelimit memlimit
    result_dir can be absolute or releative path
    timelimit in ms
    memlimit in byte
    """ % (sys.argv[0])
    if len(sys.argv)!=6:
        print usage
        exit()

    try:
        producer = Producer(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
        producer.solve()
        while True:
            if producer.finished:
                break
            time.sleep(5)
        print 'ok'
    except Exception, e:
        print e
    except KeyboardInterrupt, e:
        print e