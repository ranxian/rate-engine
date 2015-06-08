# coding:utf8
# RATE-engine 评测任务生产者

import sys
import signal
import time
import pika
import os
import json
import pickle
import ConfigParser
import threading
import logging
import urllib
import urllib2
import traceback
from uuid import uuid4
from subprocess import call

logging.basicConfig()
config = ConfigParser.ConfigParser()
config.readfp(open('%s/producer.conf' % os.path.dirname(os.path.realpath(__file__)), 'r'))

ENROLL_BLOCK_SIZE = config.getint('rate-server', 'ENROLL_BLOCK_SIZE')
MATCH_BLOCK_SIZE = config.getint('rate-server', 'MATCH_BLOCK_SIZE')
RATE_ROOT = config.get('rate-server', 'RATE_ROOT')
MAX_WORKING_ENROLL_SUBTASKS = config.getint('rate-server', 'MAX_WORKING_ENROLL_SUBTASKS')
MAX_WORKING_MATCH_SUBTASKS = config.getint('rate-server', 'MAX_WORKING_MATCH_SUBTASKS')
RABBITMQ_HOST=config.get('rate-server', 'RABBITMQ_HOST')

ENROLL_PROGRESS_PART = 0.3

class Producer:
    def __init__(self, buuid, auuid, task_uuid, timelimit, memlimit):
        benchmark_dir = "/".join((RATE_ROOT, 'benchmarks', buuid))
        algorithm_dir = "/".join(('algorithms', auuid))
        result_dir = "/".join((RATE_ROOT, 'tasks', task_uuid))

        self.buuid = buuid
        self.auuid = auuid
        self.task_uuid = task_uuid
        self.result_dir = result_dir
        self.timelimit = timelimit
        self.memlimit = memlimit
        self.finished = False
        self.host = RABBITMQ_HOST

        self.benchmark_file_path = "/".join((benchmark_dir, 'benchmark_bxx.txt'))
        self.uuid_table_file_path = "/".join((benchmark_dir, 'uuid_table.txt'))
        self.log_file_path = "/".join((result_dir, 'log.json'))
        self.template_dir = "/".join((result_dir, "templates"))
        self.relative_template_dir = "/".join(('tasks', task_uuid, 'templates'))

        self.uuid_bxx_table = {}
        self.bxx_uuid_table = {}
        self.enrollEXE = "/".join((algorithm_dir, 'enroll.exe'))
        self.matchEXE = "/".join((algorithm_dir, 'match.exe'))

        self.enroll_result_all_filepath = '/'.join((result_dir, 'enroll_result.txt'))
        self.genuine_result_filepath = '/'.join((result_dir, 'genuine.txt'))
        self.imposter_result_filepath = '/'.join((result_dir, 'imposter.txt'))

        self.enroll_state = {}
        self.match_state = {}
        self.enroll_failed_uuids = []
        self.enroll_submitted = 0
        self.enroll_finished = 0
        self.enroll_failed = 0
        self.submitting_enroll = False
        self.match_failed_uuids = []
        self.match_submitted = 0
        self.match_finished = 0
        self.match_failed = 0
        self.submitting_match = False

        self.enroll_lock = threading.Lock() # should be locked whether submitting or receiving result
        self.match_lock = threading.Lock()
        self.heart_beat_lock = threading.Lock()

        self.finished = False

    def solve(self):
        self.prepare()
        if not self.finished:
            self.doEnroll()
            self.doMatch()
            self.preprocess()
            self.finished = True
            self.dump_log()

    def preprocess(self):
        print '[PRE-PROCESS] begin'
        os.system(' '.join(("sort -k5,5", "-o", self.genuine_result_filepath+".sorted", self.genuine_result_filepath)))
        os.system(' '.join(("sort -k5,5", "-o", self.imposter_result_filepath+".sorted", self.imposter_result_filepath)))
        os.system(' '.join(("sort -k5,5", "-r", "-o", self.imposter_result_filepath+".rev.txt", self.imposter_result_filepath)))
        print '[PRE-PROCESS] sorted'
        os.system(' '.join(("mv", self.genuine_result_filepath + ".sorted", self.genuine_result_filepath)))
        os.system(' '.join(("mv", self.imposter_result_filepath + ".sorted", self.imposter_result_filepath)))
        print '[PRE-PROCESS] done'

    def delete_queues(self):
        conn = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
        channel = conn.channel()
        try:
            print 'delete queue', self.job_qname
            channel.queue_delete(queue=self.job_qname)
        except Exception, e:
            trackback.print_exc()
            pass
        try:
            channel.queue_delete(queue=self.enroll_result_qname)
        except Exception:
            pass
        try:
            channel.queue_delete(queue=self.match_result_qname)
        except Exception:
            pass

    # 记录任务中间状态
    def dump_log(self):
        information = {
            'task_uuid': self.uuid,
            'enroll_finished': self.enroll_finished,
            'enroll_submitted': self.enroll_submitted,
            'enroll_failed': self.enroll_failed,
            'enroll_failed_uuids': self.enroll_failed_uuids,
            'enroll_state': self.enroll_state,
            'match_finished': self.match_finished,
            'match_submitted': self.match_submitted,
            'match_failed': self.match_failed,
            'match_failed_uuids': self.match_failed_uuids,
            'match_state': self.match_state,
            'finished': self.finished
        }
        with open(self.log_file_path, 'w') as f:
            f.write(json.dumps(information, indent=2, sort_keys=True))

    # 读取任务中间状态
    def load_log(self):
        with open(self.log_file_path, 'r') as f:
            information = json.load(f)
            self.uuid = information["task_uuid"]
            self.enroll_finished = information["enroll_finished"]
            self.enroll_submitted = information["enroll_submitted"]
            self.enroll_failed = information["enroll_failed"]
            self.enroll_failed_uuids = information["enroll_failed_uuids"]
            self.enroll_state = information["enroll_state"]

            self.match_finished = information["match_finished"]
            self.match_submitted = information["match_submitted"]
            self.match_failed = information["match_failed"]
            self.match_failed_uuids = information["match_failed_uuids"]
            self.match_state = information["match_state"]

            self.finished = information["finished"]

    def make_heart_beat(self):
        while True:
            with self.heart_beat_lock:
                try:
                    self.conn.process_data_events()
                except Exception, e:
                    pass
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
                pass
                # raise Exception("file does not exists: %s" % fpath)
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
        files.append(self.matchEXE)
        for i in match_block:
            files.append(i['file1'])
            files.append(i['file2'])
        subtask['files'] = files
        subtask['block_no'] = block_no
        self.submit(subtask)

    # 回收 enroll 任务
    def enrollCallBack(self, ch, method, properties, body):
        def enroll_result_filename(block_no):
            return '/'.join((self.result_dir, 'enroll_result_%d.txt' % (block_no)))

        with self.enroll_lock:
            result = pickle.loads(body)
            block_no = result['block_no']
            result_f = open(enroll_result_filename(block_no), 'w')
            result_all_f = open(self.enroll_result_all_filepath, 'a+')
            enroll_failed_f = open('/'.join((self.result_dir, 'enroll_failed.txt')), 'a+')

            for rawResult in result['results']:
                result_f.write('%s %s\n' % (rawResult['uuid'], rawResult['result']))
                result_all_f.write('%s %s\n' % (rawResult['uuid'], rawResult['result']))
                if rawResult['result']=='failed':
                    self.enroll_failed += 1
                    self.enroll_failed_uuids.append(rawResult['uuid'])
                    enroll_failed_f.write('%s\n' % (rawResult['uuid']))

            self.enroll_finished += 1

            enroll_failed_f.close()
            result_all_f.close()
            result_f.close()

            self.enroll_state[result['block_no']] = True
            self.dump_log()
            ch.basic_ack(delivery_tag=method.delivery_tag)

            print "[ENROLL] enroll result [%s] finished/submitted [%d/%d] failed [%d]" % \
                (result['block_no'], self.enroll_finished, self.enroll_submitted, self.enroll_failed)

            if (not self.submitting_enroll) and self.enroll_finished == self.enroll_submitted:
                ch.stop_consuming()

    def matchCallBack(self, ch, method, properties, body):
        def match_result_filename(block_no):
            return '/'.join((self.result_dir, 'match_result_%d.txt' % (block_no)))

        with self.match_lock:
            result = pickle.loads(body)
            block_no = result['block_no']
            result_f = open(match_result_filename(block_no), 'w')
            genuine_result_f = open(self.genuine_result_filepath, 'a+')
            imposter_result_f = open(self.imposter_result_filepath, 'a+')
            match_failed_f = open('/'.join((self.result_dir, 'match_failed.txt')), 'a+')
            ch.basic_ack(delivery_tag=method.delivery_tag)
            for rawResult in result['results']:
                bxxid1 = self.uuid_bxx_table[rawResult['uuid1']]
                bxxid2 = self.uuid_bxx_table[rawResult['uuid2']]
                if rawResult['result'] == 'failed':
                    self.match_failed_uuids.append('%s %s' % (rawResult['uuid1'], rawResult['uuid2']))
                    self.match_failed += 1
                    match_failed_f.write('%s %s %s\n' % (bxxid1, bxxid2, rawResult['match_type']))
                    line = '%s %s %s %s\n' % (bxxid1, bxxid2, rawResult['match_type'], rawResult['result'])
                    result_f.write(line)
                else:
                    line = '%s %s %s %s %s\n' % (bxxid1, bxxid2, rawResult['match_type'], rawResult['result'], rawResult['score'])
                    result_f.write(line)
                    if rawResult['match_type'] == 'G':
                        genuine_result_f.write(line)
                    else:
                        imposter_result_f.write(line)

            self.match_finished += 1
            match_failed_f.close()
            imposter_result_f.close()
            genuine_result_f.close()
            result_f.close()

            self.match_state[result['block_no']] = True
            self.dump_log()

            print "[MATCH] match result [%s] finished [%d/%d=%d%%] failed [%d]" % (result['block_no'], self.match_finished, 
                                        self.match_submitted, float(self.match_finished) / self.match_submitted * 100,
                                        self.match_failed)

            if (not self.submitting_match) and self.match_finished == self.match_submitted:
                ch.stop_consuming()

    def waitForEnrollResults(self):
        print '[ENROLL] waiting for enroll results'
        conn = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        ch = conn.channel()
        self.enroll_result_ch = ch
        ch.queue_declare(queue=self.enroll_result_qname, durable=False, exclusive=False, auto_delete=False)
        ch.basic_consume(self.enrollCallBack, queue=self.enroll_result_qname)
        try:
            ch.start_consuming()
            ch.queue_delete(queue=self.enroll_result_qname)
        except Exception, e:
            pass

    def waitForMatchResults(self):
        print '[MATCH] waiting for match results'
        conn = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        ch = conn.channel()
        self.match_result_ch = ch
        ch.queue_declare(queue=self.match_result_qname, durable=False, exclusive=False, auto_delete=False)
        ch.basic_consume(self.matchCallBack, queue=self.match_result_qname)
        try:
            ch.start_consuming()
            ch.queue_delete(queue=self.match_result_qname)
        except Exception, e:
            print e
            pass

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
                    if self.enroll_state.get(str(block_no)) != None:
                        block_no += 1
                        enroll_block = []
                        continue
                    else:
                        with self.enroll_lock:
                            self.submitEnroll(enroll_block, block_no)
                            block_no += 1
                            self.enroll_submitted += 1
                            enroll_block = []

        with self.enroll_lock:
            if len(enroll_block) != 0 and (self.enroll_state.get(str(block_no)) == None):
                self.submitEnroll(enroll_block, block_no)
                block_no += 1
                self.enroll_submitted += 1

        self.submitting_enroll = False
        while self.enroll_finished != self.enroll_submitted:
            time.sleep(5)
        print '[ENROLL] finished, failed %d\n' % self.enroll_failed

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

                f1 = '%s/%s.t' % (self.relative_template_dir, u1)
                f2 = '%s/%s.t' % (self.relative_template_dir, u2)
                t = { 'uuid1':u1, 'uuid2':u2, 'file1':f1, 'file2':f2, 'match_type':gOrI }
                match_block.append(t)
                if len(match_block) == MATCH_BLOCK_SIZE:
                    if self.match_state.get(str(block_no)) != None:
                        block_no += 1
                        match_block = []
                    else:
                        self.submitMatch(match_block, block_no)
                        block_no += 1
                        self.match_submitted += 1
                        match_block = []
                        if block_no % 10 == 0:
                            print "[MATCH] [%d*%d] matches has been submitted" % (self.match_submitted, MATCH_BLOCK_SIZE)

        with self.match_lock:
            if len(match_block)!=0 and (self.match_state.get(str(block_no)) == None):
                self.submitMatch(match_block, block_no)
                block_no += 1
                self.match_submitted += 1
                match_block = []
            self.submitting_match = False
        benchmarkf.close()

        print "[MATCH] %d matches" % self.match_submitted
        print "[MATCH] all matches submitted, waiting for all results"

        while True:
            with self.match_lock:
                if self.match_submitted == self.match_finished:
                    break
            time.sleep(5)
        print "[MATCH] finished, failed %d\n" % self.match_failed

    def prepare(self):
        print '[PREPARE] begin'

        if os.path.exists(self.log_file_path):
            print '[PREPARE] restore task from log'
            self.load_log()
            if not self.enroll_submitted == 0:
                print '[PREPARE] Enroll progress: %d/%d=%d%% [finished/submitted], %d failed' % \
                    (self.enroll_finished, self.enroll_submitted,
                     float(self.enroll_finished) / self.enroll_submitted * 100, self.enroll_failed)
            if not self.match_submitted == 0:
                print '[PREPARE] Match progress: %d/%d=%d%% [finished/submitted], %d failed' % \
                    (self.match_finished, self.match_submitted,
                     float(self.match_finished) / self.match_submitted * 100, self.match_failed)
        else:
            if not os.path.isdir(self.result_dir):
                os.makedirs(self.result_dir)
            if not os.path.exists(self.template_dir):
                os.makedirs(self.template_dir)
            if not os.path.exists(self.enroll_result_all_filepath):
                pass

            self.uuid = uuid4().__str__()
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

        self.conn = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        self.ch = self.conn.channel()
        self.ch.queue_declare(queue=self.job_qname, durable=False, exclusive=False, auto_delete=False)
        # Notify the creation of task
        url = 'http://rate.pku.edu.cn/admin/add_task?uuid=' + self.uuid
        res = urllib2.urlopen(url)

        hbt = threading.Thread(target=self.make_heart_beat)
        hbt.daemon = True
        hbt.start()
        print '[PREPARE] start heart beat thread'
        print "[PREPARE] finished\n"

producer = None

def signal_term_handler(signal, frame):
    global producer
    print 'Killed by other process. Trying to exit gracefully'
    producer.delete_queues()
    url = 'http://rate.pku.edu.cn/admin/remove_task?uuid=' + producer.uuid
    res = urllib2.urlopen(url)
    sys.exit(0)

signal.signal(signal.SIGTERM, signal_term_handler)

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

    producer = Producer(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    producer.solve()

    try:
        while True:
            if producer.finished:
                break
            time.sleep(5)
    except Exception, e:
        print e
        traceback.print_exc()
    except KeyboardInterrupt, e:
        print e
    finally:
        producer.delete_queues()
        # notify the termination of task
        url = 'http://rate.pku.edu.cn/admin/remove_task?uuid=' + producer.uuid
        res = urllib2.urlopen(url)
