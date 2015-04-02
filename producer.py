#coding:utf8
# RATE-engine 评测任务生产者

import base64
import re
import json
import logging
logging.basicConfig()
from pika.adapters.tornado_connection import TornadoConnection
import shutil
import threading
import os
import pika
import pickle
import time
from uuid import uuid4
import ConfigParser
from matchresult2bxx import matchresult2bxx
from task_bitmap import BMManager
import sys

config = ConfigParser.ConfigParser()
config.readfp(open('%s/producer.conf' % os.path.dirname(os.path.realpath(__file__)), 'r'))

ENROLL_BLOCK_SIZE = config.getint('rate-server', 'ENROLL_BLOCK_SIZE')
MATCH_BLOCK_SIZE = config.getint('rate-server', 'MATCH_BLOCK_SIZE')
RATE_ROOT = config.get('rate-server', 'RATE_ROOT')
MAX_WORKING_ENROLL_SUBTASKS = config.getint('rate-server', 'MAX_WORKING_ENROLL_SUBTASKS')
MAX_WORKING_MATCH_SUBTASKS = config.getint('rate-server', 'MAX_WORKING_MATCH_SUBTASKS')

# 生成给 rabbitmq 用的 match 运算的 key
def formMatchKey(algorithm_uuid, u1, u2):
    u1 = u1.replace('-', '').lower()
    u2 = u2.replace('-', '').lower()
    if u1>u2:
        t = u1
        u1 = u2
        u2 = t
    algorithm_uuid = algorithm_uuid.replace('-', '').lower()
    return "m#%s#%s#%s" % (algorithm_uuid[-12:], u1[-12:], u2[-12:])

# 生成给 rabbitmq 用的 enroll 运算的 key
def formEnrollKey(algorithm_uuid, u):
    return "e#%s#%s" % (algorithm_uuid[-12:], u[-12:])

class RateProducer:
    def __init__(self, benchmark_uuid, result_file_dir, algorithm_uuid, timelimit, memlimit):
        # 准备各种变量
        benchmark_dir = "/".join((RATE_ROOT, 'benchmarks', benchmark_uuid))
        algorithm_dir = "/".join(('algorithms', algorithm_uuid))
        self.host = config.get('rate-server', 'RABBITMQ_HOST')
        self.benchmark_dir = benchmark_dir
        self.result_file_dir = result_file_dir
        if not os.path.isdir(result_file_dir):
            os.makedirs(result_file_dir)
        self.benchmark_bxx_file_path = "/".join((benchmark_dir, 'benchmark_bxx.txt'))
        self.uuid_table_file_path = "/".join((benchmark_dir, 'uuid_table.txt'))
        self.uuid_bxx_table = {}
        self.bxx_uuid_table = {}
        self.enrollEXE = "/".join((algorithm_dir, 'enroll.exe'))
        self.matchEXE = "/".join((algorithm_dir, 'match.exe'))
        self.timelimit = timelimit
        self.memlimit = memlimit

        self.log_file_path = "/".join((result_file_dir, 'log.json'))
        self.enroll_result_file_path = "/".join((result_file_dir, 'enroll_result.txt'))
        self.match_result_file_path = "/".join((result_file_dir, 'match_result_bxx.txt'))

        self.already_match_number = 0
        self.all_finished = False

        self.results = {}
        self.uuid = ""
        self.enroll_tasks_state = {}
        self.match_tasks_state = {}
        self.working_task_uuids = {}

        self.enroll_subtask_uuis = []
        self.match_subtask_uuids = []
        self.finished_enroll_subtask_uuids = []
        self.finished_match_subtask_uuids = []
        self.finished_enroll_uuids = set()
        self.failed_enroll_uuids = set()
        self.failed_match_uuids = set()
        self.submitting_enroll = False
        self.submitting_match = False
        self.enroll_lock = threading.Lock() # should be locked whether submitting or receiving result
        self.match_lock = threading.Lock()
        self.heart_beat_lock = threading.Lock()
        self.enroll_result_qname = 'results-enroll-%s' % (self.uuid,)
        self.match_result_qname = 'results-match-%s' % (self.uuid,)
        self.enroll_uuids = set()

        # 连接 rmq 服务器
        self.conn = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        self.ch = self.conn.channel()
        print "queue server connected"

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

    # 提交 enroll 任务
    def submitEnrollBlock(self, l):
        subtask = self.genSubtask(l, 'enroll')
        files = []
        files.append(self.enrollEXE)
        files.append(self.matchEXE)
        for i in l:
            files.append(i['file'])
        subtask['files'] = files
        self.submit(subtask)

    # 提交 match 任务
    def submitMatchBlock(self, l):
        subtask = self.genSubtask(l, 'match')
        files = []
        files.append(self.enrollEXE)
        files.append(self.matchEXE)
        for i in l:
            files.append(i['file1'])
            files.append(i['file2'])
        subtask['files'] = files
        self.match_subtask_uuids.append(subtask['subtask_uuid'])
        self.submit(subtask)

    # 生成小任务 - 一个个的 match 或者 enroll 任务
    def genSubtask(self, tinytasks, taskType, reGen=False):
        subtask = {
                'timelimit'     : self.timelimit,
                'memlimit'      : self.memlimit,
                'tinytasks'     : tinytasks,
                'producer_uuid' : self.uuid,
                'enrollEXE'     : self.enrollEXE,
                'matchEXE'      : self.matchEXE,
                'type'          : taskType,
               }
        if not reGen:
            subtask[len(sell.enroll_tasks_state)] = False
        return subtask

    # 准备
    # 初始化及恢复中间结果
    def prepare(self):
        print "[PREPARING] preparing queues"
        self.ch.queue_declare(queue='jobs', durable=False, exclusive=False, auto_delete=False)

        # 尝试恢复任务日志文件
        if os.path.exists(self.log_file_path):
            print '[PREPARING] restore from last run'
            self.load_log()
            self.enroll_result_file = open(self.enroll_result_file_path, 'a')
            self.match_result_file = open(self.match_result_file_path, 'a')
        # 初始化
        else:
            print '[PREPARING] '
            # 生成 uuid
            self.uuid = uuid4().__str__()
            # 生成 match|enroll result file
            open(self.match_result_file_path, 'w').close()
            open(self.enroll_result_file_path, 'w').close()
            self.enroll_result_file = open(self.enroll_result_file_path, 'a')
            self.match_result_file = open(self.match_result_file_path, 'a')
            if not os.path.exists('/'.join((RATE_ROOT, 'temp', self.uuid[-12:]))):
                os.makedirs("/".join((RATE_ROOT, 'temp', self.uuid[-12:])))
                for i in range(16*16):
                    tdir = str(hex(i+256))[-2:]
                    os.mkdir("/".join((RATE_ROOT, 'temp', self.uuid[-12:], tdir)))
            # 生成日志文件
            self.dump_log()

        # 心跳线程
        hbt = threading.Thread(target=self.make_heart_beat)
        hbt.daemon = True
        hbt.start()
        print '[PREPARING] start heart beat thread'
        print "[PREPARING] -- prepare finished"

    def doEnroll(self):
        print '[ENROLL] begin enroll'
        lines_proceeded = 0

        self.submitting_enroll = True
        i = 0 # i j is for counting in case to print messages with i%xxx=0
        j = 0
        enroll_block = []
        enrollf = open(self.uuid_table_file_path, 'r')
        with self.enroll_lock:
            enroll_result_thread = threading.Thread(target=self.waitForEnrollResults)
            enroll_result_thread.daemon = True
            enroll_result_thread.start()
        wait = False
        while True:
            if wait == True:
                time.sleep(60)
                wait = False
            with self.enroll_lock:
                working_enroll_subtasks = len(self.enroll_subtask_uuids)-len(self.finished_enroll_subtask_uuids)

                if working_enroll_subtasks >= MAX_WORKING_ENROLL_SUBTASKS:
                    print "[ENROLL] %d enroll queue full, wait for 1 min" % (working_enroll_subtasks, )
                    wait = True
                    continue
                a = enrollf.readline()
                lines_proceeded += 1
                if lines_proceeded%1000==0:
                    print "[ENROLL] %d enrolls proceeded" % lines_proceeded
                    pass

                if len(a)==0:
                    break

                # bxxid, uuid, sample 文件地址
                (bxx, u, f) = a.strip().split(' ')
                f = "/".join(('samples', f))

                if u not in self.uuid_bxx_table:
                    self.uuid_bxx_table[u] = bxx
                    self.bxx_uuid_table[bxx] = u
                    self.enroll_uuids.add(u)
                    t = {'uuid':u, 'file': f }
                    #print>>enroll_log_file, u, f
                    enroll_block.append(t)
                    if len(enroll_block) == ENROLL_BLOCK_SIZE:
                        self.submitEnrollBlock(enroll_block)
                        enroll_block = []
                        j = j+1
                        if j%10 == 0:
                            print "[ENROLL] [%d*%d=%d] enrolls has been submitted" % (j, ENROLL_BLOCK_SIZE, j * ENROLL_BLOCK_SIZE)

        with self.enroll_lock:
            if len(enroll_block)!=0:
                self.submitEnrollBlock(enroll_block)
                enroll_block = []
                enrollf.close()
            if len(self.finished_enroll_subtask_uuids) == len(self.enroll_subtask_uuids):
                print "[ENROLL] enroll workers finished before producer reach this line"
                try:
                  if hasattr(self, 'enroll_result_ch'):
                    self.enroll_result_ch.stop_consuming()
                  else:
                    print '[ENROLL] no busy enroll result channel yet'
                except Exception, e:
                    print '[ENROLL] can cansel consuming'
                    print e
            self.submitting_enroll = False

        print "[ENROLL] %d enrolls" % len(self.enroll_uuids)
        print "[ENROLL] all enrolls submitted, waiting for all results"
        print '[ENROLL] wait for another 5 secs for enroll results to be tranferred'
        time.sleep(5)
        print "[ENROLL] enroll finished, failed %d" % len(self.failed_enroll_uuids)

    def doMatch(self):
        self.submitting_match = True
        l = []
        i = 0
        self.submitted_match_count = 0
        self.failed_match_count = 0
        benchmarkf = open(self.benchmark_bxx_file_path, 'r')
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
                working_match_subtasks = len(self.match_subtask_uuids)-len(self.finished_match_subtask_uuids)
                if working_match_subtasks >= MAX_WORKING_MATCH_SUBTASKS:
                    print "%d match queue full, wait for 5 sec" % (working_match_subtasks, )
                    wait = True
                    continue
                i = i+1

                a = benchmarkf.readline() # 11 22 I
                if len(a)==0:
                    break

                if i%100000==0:
                    print "%d matches proceeded" % (i,)

                (bxx1, bxx2, gOrI) = a.strip().split(' ')[:3]
                u1 = self.bxx_uuid_table[bxx1]
                u2 = self.bxx_uuid_table[bxx2]

                # Check bitmap file
                if self.manager.query_line(a) == 1:
                  self.already_match_number += 1
                  if self.already_match_number % 100000 == 0:
                    print 'already', self.already_match_number, 'matched'
                  continue

                if u1 in self.failed_enroll_uuids or u2 in self.failed_enroll_uuids:
                    continue
                f1 = 'temp/%s/%s/%s.t' % (self.uuid[-12:], u1[-12:-10], u1[-10:])
                f2 = 'temp/%s/%s/%s.t' % (self.uuid[-12:], u2[-12:-10], u2[-10:])
                t = { 'uuid1':u1, 'uuid2':u2, 'file1':f1, 'file2':f2, 'match_type':gOrI }
                l.append(t)
                self.submitted_match_count += 1
                if len(l) == MATCH_BLOCK_SIZE:
                    self.submitMatchBlock(l)
                    l = []
                    if len(self.match_subtask_uuids)%10 == 0:
                        print "[%d*%d] matches has been submitted" % (len(self.match_subtask_uuids), MATCH_BLOCK_SIZE)

        all_match_finished = False
        with self.match_lock:
            if len(l)!=0:
                self.submitMatchBlock(l)
                l = []
            if len(self.match_subtask_uuids) == len(self.finished_match_subtask_uuids):
                print "match workers finished before producer reach this line"
                try:
                    self.match_result_ch.stop_consuming()
                except Exception, e:
                    all_match_finished = True
                    print e
            self.submitting_match = False
        benchmarkf.close()

        print "%d matches" % self.submitted_match_count
        print "all matches submitted, waiting for all results"
        if not all_match_finished:
          match_result_thread.join()
        print "match finished, failed %d" % self.failed_match_count
        with self.heart_beat_lock:
          self.all_finished = True

    def solve(self):
        try:
            self.prepare()
            self.doEnroll()
            self.doMatch()
            state_file = open(self.state_file_path, 'w')
            state_file.write("1\n")
            state_file.close()
    #        self.generateResults()
            self.cleanUp()
        except Exception, e:
            print e


    def cleanUp(self):
        print "cleaning up"
        try:
            temp_dir = "/".join((RATE_ROOT, 'temp', self.uuid[-12:]))
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
        except Exception, e:
            print e

        self.ch.exchange_declare(exchange='jobs-cleanup-exchange', type='fanout')
        cleanup_dir = "/".join(('temp', self.uuid[-12:]))
        self.ch.basic_publish(exchange='jobs-cleanup-exchange', routing_key='', body=pickle.dumps(cleanup_dir))

    # 向 Rabbitmq 提交任务
    def submit(self, subtask):
        self.heart_beat_lock.acquire()
        if subtask==None:
            return
        for fpath in subtask['files']:
            fpath = "/".join((RATE_ROOT, fpath))
            if not os.path.exists(fpath):
                raise Exception("file does not exists: %s" % fpath)
        self.ch.basic_publish(exchange='', routing_key='jobs', body=pickle.dumps(subtask))
        self.heart_beat_lock.release()

    # This method is just to let rabbitmq know we are alive
    def make_heart_beat(self):
      while True:
        with self.heart_beat_lock:
          self.conn.process_data_events()
        time.sleep(10)
        if self.all_finished:
          break

    def enrollCallBack(self, ch, method, properties, body):
        progress_part = 0.3
        with self.enroll_lock:
            result = pickle.loads(body)
            self.finished_enroll_subtask_uuids.append(result['subtask_uuid'])
            ch.basic_ack(delivery_tag=method.delivery_tag)
            for rawResult in result['results']:
                uuid_ = rawResult['uuid']
                self.finished_enroll_uuids.add(uuid_)
                print>>self.enroll_result_file, "%s %s" % (uuid_, rawResult['result'])
                self.enroll_result_file.flush()
                if rawResult['result']=='failed':
                    self.failed_enroll_uuids.add(uuid_)
            print "enroll result [%s] finished [%d/%d] failed/total [%d/%d]" % (result['subtask_uuid'][:8], len(self.finished_enroll_subtask_uuids), len(self.enroll_subtask_uuids), len(self.failed_enroll_uuids), len(self.enroll_uuids))

            # write progress
            state_file = open(self.state_file_path, 'w')
            state_file.write("%f\n" % (progress_part * float(len(self.finished_enroll_subtask_uuids)) / len(self.enroll_subtask_uuids)))
            state_file.close()
#            self.enroll_result_file.flush()

            if (not self.submitting_enroll) and len(self.finished_enroll_subtask_uuids)==len(self.enroll_subtask_uuids):
                print "enrollCallBack stop consuming"
                ch.stop_consuming()

    def matchCallBack(self, ch, method, properties, body):
        with self.match_lock:
            print('loads body')
            result = pickle.loads(body)
            print('basic_ack')
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print('write result')
            for rawResult in result['results']:
                bxxid1 = self.uuid_bxx_table[rawResult['uuid1']]
                bxxid2 = self.uuid_bxx_table[rawResult['uuid2']]
                # Add match result to bitmap
                aline = '%s %s' % (bxxid1, bxxid2)
                self.manager.update_bitmap([aline])

            self.finished_match_subtask_uuids.append(result['subtask_uuid'])
            print "match result [%s] finished [%d/%d=%d%%] failed [%d/%d]" % (result['subtask_uuid'][:8], len(self.finished_match_subtask_uuids), len(self.match_subtask_uuids), 100*len(self.finished_match_subtask_uuids)/len(self.match_subtask_uuids), self.failed_match_count, self.submitted_match_count)
            print 'write status'
            state_file = open(self.state_file_path, 'w')
            state_file.write("%f\n" % (0.3 + 0.7 * float(len(self.finished_match_subtask_uuids))/len(self.match_subtask_uuids)))
            state_file.close()

            print 'flush status'
            self.match_result_file.flush()
            if (not self.submitting_match) and len(self.finished_match_subtask_uuids)==len(self.match_subtask_uuids):
                ch.stop_consuming()

    def waitForEnrollResults(self):
        print 'waiting for enroll results'
        conn = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        ch = conn.channel()
        self.enroll_result_ch = ch
        ch.queue_declare(queue=self.enroll_result_qname, durable=False, exclusive=False, auto_delete=False)
        print 'queue declared'
        if (not self.submitting_enroll) and len(self.finished_enroll_subtask_uuids)==len(self.enroll_subtask_uuids):
            print 'no consume'
            pass
        else:
            print self.submitting_enroll
            print len(self.finished_enroll_subtask_uuids)
            print len(self.enroll_subtask_uuids)

            ch.basic_consume(self.enrollCallBack, queue=self.enroll_result_qname)
            print 'start_consuming'
            ch.start_consuming()
        print 'going to end enroll result queue'
        ch.queue_delete(queue=self.enroll_result_qname)
        print 'wait finished'

    def waitForMatchResults(self):
        print 'waiting for match results'
        conn = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        ch = conn.channel()
        self.match_result_ch = ch
        ch.queue_declare(queue=self.match_result_qname, durable=False, exclusive=False, auto_delete=False)
        if (not self.submitting_match) and len(self.finished_match_subtask_uuids)==len(self.match_subtask_uuids):
            pass
        else:
            ch.basic_consume(self.matchCallBack, queue=self.match_result_qname)
            print 'match start consuming'
            ch.start_consuming()
            print "match_result_ch consumed"
        ch.queue_delete(queue=self.match_result_qname)

        self.match_result_file.close()

        # matchresult2bxx(self.benchmark_dir, self.result_file_dir)
#        conn.close()

if __name__=='__main__':
    usage = """Usage:
    python %s benchmark_uuid result_dir algorithm_uuid timelimit memlimit
    host is the host where queue server is on
    result_dir can be absolute or releative path
    timelimit in ms
    memlimit in byte
    """ % (sys.argv[0])
    if len(sys.argv)!=6:
        print usage
        exit()

    try:
        p = RateProducer(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
        p.solve()
        while True:
            if p.all_finished:
                break
        print 'ok'
    except Exception, e:
        print e
    except KeyboardInterrupt, e:
        print e