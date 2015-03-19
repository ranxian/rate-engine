#coding:utf8
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
import uuid
import ConfigParser
from matchresult2bxx import matchresult2bxx
from task_bitmap import BMManager
import sys

config = ConfigParser.ConfigParser()
config.readfp(open('%s/producer.conf' % os.path.dirname(os.path.realpath(__file__)), 'r'))

#TODO cache the templates only when the template is small
USE_MEMCACHE = config.getint('rate-server', 'USE_MEMCACHE')
if USE_MEMCACHE:
    import memcache
    memcache_hosts = ['localhost:11211', ]#'162.105.30.164:11211']

def getMemcacheConn(host):
#    return None
    # FIXME it's a temp work around
    return memcache.Client(memcache_hosts, debug=0)
#    return memcache.Client(["%s:11211" % host,], debug=0)

ENROLL_BLOCK_SIZE = config.getint('rate-server', 'ENROLL_BLOCK_SIZE')
MATCH_BLOCK_SIZE = config.getint('rate-server', 'MATCH_BLOCK_SIZE')
PRODUCER_RATE_ROOT = config.get('rate-server', 'PRODUCER_RATE_ROOT')
MAX_WORKING_ENROLL_SUBTASKS = config.getint('rate-server', 'MAX_WORKING_ENROLL_SUBTASKS')
MAX_WORKING_MATCH_SUBTASKS = config.getint('rate-server', 'MAX_WORKING_MATCH_SUBTASKS')
IGNORE_MATCH = config.getint('rate-server', 'IGNORE_MATCH')

def formMatchKey(algorithm_version_uuid, u1, u2):
    u1 = u1.replace('-', '').lower()
    u2 = u2.replace('-', '').lower()
    if u1>u2:
        t = u1
        u1 = u2
        u2 = t
    algorithm_version_uuid = algorithm_version_uuid.replace('-', '').lower()
    return "m#%s#%s#%s" % (algorithm_version_uuid[-12:], u1[-12:], u2[-12:])

def formEnrollKey(algorithm_version_uuid, u):
    return "e#%s#%s" % (algorithm_version_uuid[-12:], u[-12:])

class RateProducer:
    def __init__(self, host, benchmark_file_dir, result_file_dir, algorithm_version_dir, timelimit, memlimit):
        self.host = host
        self.benchmark_file_dir = benchmark_file_dir
        self.result_file_dir = result_file_dir
        self.benchmark_bxx_file_path = "/".join((benchmark_file_dir, 'benchmark_bxx.txt'))
        self.uuid_table_file_path = "/".join((benchmark_file_dir, 'uuid_table.txt'))
        self.uuid_bxx_table = {}
        self.bxx_uuid_table = {}
        self.enrollEXE = "/".join((algorithm_version_dir, 'enroll.exe')).replace('\\', '/')
        self.enrollEXEUUID = [ v for v in self.enrollEXE.split('/') if v!="" ][-2].replace('-', '')
        self.matchEXE = "/".join((algorithm_version_dir, 'match.exe')).replace('\\','/')
        self.matchEXEUUID = [ v for v in self.matchEXE.split('/') if v!="" ][-2].replace('-','')
        self.timelimit = timelimit
        self.memlimit = memlimit
        self.ongoing_file_path = "/".join((result_file_dir, 'task.ongoing'))
        self.bmf_path = "/".join((result_file_dir, 'task.bm'))
        self.state_file_path = "/".join((result_file_dir, "state.txt"))

        self.already_match_number = 0
#        self.conn = TornadoConnection(pika.ConnectionParameters(host))
        self.conn = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        self.ch = self.conn.channel()
        print "queue server connected"
        self.results = {}
        self.uuid = self.get_uuid()
        self.enroll_subtask_uuids = []
        self.match_subtask_uuids = []
        self.finished_enroll_subtask_uuids = []
        self.finished_match_subtask_uuids = []
        if not os.path.isdir(result_file_dir):
            os.makedirs(result_file_dir)
        self.finished_enroll_uuids = set()
#        self.finished_match_uuids = set()
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

        self.enroll_result_file_path = "/".join((result_file_dir, 'enroll_result.txt'))
        self.match_result_file_path = "/".join((result_file_dir, 'match_result_bxx.txt'))
        self.all_finished = False


        self.wa_first_result = True

    def submitEnrollBlock(self, l):
        subtask = self.genSubtask(l, 'enroll')
        files = []
        files.append(self.enrollEXE)
        files.append(self.matchEXE)
        for i in l:
            files.append(i['file'])
        subtask['files'] = files
        self.enroll_subtask_uuids.append(subtask['subtask_uuid'])
        self.submit(subtask)

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

    def genSubtask(self, tinytasks, taskType):
        subtask = {
                'timelimit'     : self.timelimit,
                'memlimit'      : self.memlimit,
                'tinytasks'     : tinytasks,
                'producer_uuid' : self.uuid,
                'enrollEXE'     : self.enrollEXE,
                'matchEXE'      : self.matchEXE,
                'type'          : taskType,
               }
        subtask_uuid = uuid.uuid4().__str__()
        subtask['subtask_uuid'] = subtask_uuid
        return subtask

    def get_uuid(self):
        uuid_ = uuid.uuid4().__str__()
        print 'Check ongoing_file_path'
        if os.path.exists(self.ongoing_file_path):
            print 'ongoing_file exists, read uuid from it'
            f = open(self.ongoing_file_path, 'r')
            uuid_ = f.readline().rstrip("\n")
            f.close()
        return uuid_

    def prepare(self):
        print "preparing queues"
        self.ch.queue_declare(queue='jobs', durable=False, exclusive=False, auto_delete=False)

        print "Init BMManager"
        nline = 0
        uuid_table_f = open(self.uuid_table_file_path, 'r')
        while True:
            line = uuid_table_f.readline()
            if line == '' or line == "\n":
                break
            nline += 1
        uuid_table_f.close()
        self.manager = BMManager(self.bmf_path, nline)
        print 'Inited with %d samples' % (nline)

        # prepare dirs
        print "preparing dirs on server"
        if not os.path.exists(self.result_file_dir + '/' + 'need_enroll') and os.path.exists(self.ongoing_file_path):
            print 'ongoing_file exists, no need to prepare dir'
        elif not os.path.exists('/'.join((PRODUCER_RATE_ROOT, 'temp', self.uuid[-12:]))):
            os.makedirs("/".join((PRODUCER_RATE_ROOT, 'temp', self.uuid[-12:])))
            for i in range(16*16):
                tdir = str(hex(i+256))[-2:]
                os.mkdir("/".join((PRODUCER_RATE_ROOT, 'temp', self.uuid[-12:], tdir)))
            if not os.path.exists(self.ongoing_file_path):
              print 'no ongoing_file, create and write uuid to it'
              f = open(self.ongoing_file_path, 'w')
              f.write(self.uuid + '\n')
              f.close()

        print 'See if enroll result exists'

        if not os.path.exists("/".join((self.result_file_dir, 'need_enroll'))) and os.path.exists(self.enroll_result_file_path):
            os.system("tail -n +2 " + self.enroll_result_file_path + " > /tmp/tempenroll")
            os.system("mv /tmp/tempenroll " + self.enroll_result_file_path)
            print 'previous enroll result exist, read from it'
            f = open(self.enroll_result_file_path, 'r')
            i = 0
            lastid = None
            while True:
                line = f.readline()
                if line == '' or line == "\n":
                    break
                line = line.rstrip("\n")
                uuid, result = line.split(' ')
                lastid = uuid
                if result == 'ok':
                    self.finished_enroll_uuids.add(uuid)
                else:
                    self.failed_enroll_uuids.add(uuid)
                self.enroll_uuids.add(uuid)
                i += 1
            f.close()
        else:
            open(self.enroll_result_file_path, 'w').close()
            if os.path.exists(self.result_file_dir + '/' + 'need_enroll'):
              os.remove('/'.join((self.result_file_dir, 'need_enroll')))
        self.enroll_result_file = open(self.enroll_result_file_path, 'a')
        print 'already', len(self.finished_enroll_uuids), 'enrolled'

        print 'Create match result file if needed'
        if not os.path.exists(self.match_result_file_path):
            open(self.match_result_file_path, 'w').close()
        self.match_result_file = open(self.match_result_file_path, 'a')

        # prepare heart beat thread
        hbt = threading.Thread(target=self.make_heart_beat)
        hbt.start()
        print 'Start heart beat thread'

        print "prepare finished"

    def doEnroll(self):
        if USE_MEMCACHE:
            self.enroll_cache_conn = getMemcacheConn(self.host)

        lines_proceeded = 0

        self.submitting_enroll = True
        i = 0 # i j is for counting in case to print messages with i%xxx=0
        j = 0
        l = []
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
                    print "%d enroll queue full, wait for 1 min" % (working_enroll_subtasks, )
                    wait = True
                    continue
                a = enrollf.readline()
                lines_proceeded += 1
                if lines_proceeded%1000==0:
                    print "%d enrolls proceeded" % lines_proceeded
                    pass

                if len(a)==0:
                    break

                (bxx, u, f) = a.strip().split(' ')
                f = "/".join(('samples', f))

                if u not in self.uuid_bxx_table:
                    self.uuid_bxx_table[u] = bxx
                    self.bxx_uuid_table[bxx] = u

                if u not in self.enroll_uuids:
                    if USE_MEMCACHE:
                        cache_key = formEnrollKey(self.enrollEXEUUID, u)
                        cache_value = None
                        try:
                            cache_value = json.loads(self.enroll_cache_conn.get(cache_key))
                            if cache_value[0]=='ok':
                                dest_file = open("/".join((PRODUCER_RATE_ROOT, "temp", self.uuid[-12:], u[-12:-10], u[-10:]+".t")), 'wb')
                                template = base64.b64decode(cache_value[1])
                                dest_file.write(template)
                                dest_file.close()
                                print>>self.enroll_result_file, '%s ok' % u
                            elif cache_value[0]=='failed':
                                self.failed_enroll_uuids.add(u)
                                print>>self.enroll_result_file, '%s failed' % u
                            continue
                        except Exception, e:
#                            print e
                            pass

                    self.enroll_uuids.add(u)
                    t = {'uuid':u, 'file': f }
                    #print>>enroll_log_file, u, f
                    l.append(t)
                    if len(l)==ENROLL_BLOCK_SIZE:
                        self.submitEnrollBlock(l)
                        l = []
                        j = j+1
                        if j%10 == 0:
                            print "[%d*%d=%d] enrolls has been submitted" % (j, ENROLL_BLOCK_SIZE, j * ENROLL_BLOCK_SIZE)

        with self.enroll_lock:
            if len(l)!=0:
                self.submitEnrollBlock(l)
                l = []
                enrollf.close()
                #enroll_log_file.close()
            if len(self.finished_enroll_subtask_uuids)==len(self.enroll_subtask_uuids):
                print "enroll workers finished before producer reach this line"
                try:
                  if hasattr(self, 'enroll_result_ch'):
                    self.enroll_result_ch.stop_consuming()
                  else:
                    print 'no busy enroll result channel yet'
                except Exception, e:
                    print 'can cansel consuming'
                    print e
            self.submitting_enroll = False

        print "%d enrolls" % len(self.enroll_uuids)
        print "all enrolls submitted, waiting for all results"
        enroll_result_thread.join()
        print 'wait for another 5 secs for enroll result to be tranferred'
        time.sleep(5)
        print "enroll finished, failed %d" % len(self.failed_enroll_uuids)

    def doMatch(self):
        if USE_MEMCACHE:
            self.match_cache_conn = getMemcacheConn(self.host)
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
                if i < IGNORE_MATCH:
                  continue
                (bxx1, bxx2, gOrI) = a.strip().split(' ')[:3]
                u1 = self.bxx_uuid_table[bxx1]
                u2 = self.bxx_uuid_table[bxx2]

                if USE_MEMCACHE:
                    # check if the match has already been in cache
                    # if so, output the result and continue
                    cache_key = formMatchKey(self.matchEXEUUID, u1, u2)
                    cache_value = None
#                    print cache_key
                    try:
                        cache_value = json.loads(self.match_cache_conn.get(cache_key))
                    except Exception, e:
#                        print e
                        pass
#                    print cache_value
#                    print type(cache_value)
                    if cache_value!=None:
                        if cache_value[0]=='ok':
                            print>>self.match_result_file, '%s %s %s ok %s' % (u1, u2, cache_value[1], cache_value[2])
                        elif cache_value[0]=='failed':
                            print>>self.match_result_file, '%s %s %s failed' % (u1, u2, cache_value[1])
#                        self.match_result_file.flush()
                        continue

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
            state_file = open(self.state_file_path, 'w')
            state_file.write("1\n")
            state_file.close()
            self.manager.destroy()
            raise e

    def cleanUp(self):
        print "cleaning up"
        try:
            temp_dir = "/".join((PRODUCER_RATE_ROOT, 'temp', self.uuid[-12:]))
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            need_enroll_path = "/".join((self.result_file_dir, 'need_enroll'))
            open(need_enroll_path, 'w').close()
        except Exception, e:
            print e

        self.ch.exchange_declare(exchange='jobs-cleanup-exchange', type='fanout')
        cleanup_dir = "/".join(('temp', self.uuid[-12:]))
        self.ch.basic_publish(exchange='jobs-cleanup-exchange', routing_key='', body=pickle.dumps(cleanup_dir))

    def submit(self, subtask):
        self.heart_beat_lock.acquire()
        if subtask==None:
            return
        for fpath in subtask['files']:
            fpath = "/".join((PRODUCER_RATE_ROOT, fpath))
            if not os.path.exists(fpath):
                raise Exception("file does not exists: %s" % fpath)
        self.ch.basic_publish(exchange='', routing_key='jobs', body=pickle.dumps(subtask))
        self.heart_beat_lock.release()

    def make_heart_beat(self):
# This method is just to let the server know we are alive
      while True:
        with self.heart_beat_lock:
          self.conn.process_data_events()
        print 'heart beat'
        time.sleep(5)
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

            if USE_MEMCACHE:
                for rawResult in result['results']:
                    try:
                        cache_value = [rawResult['result'], ]
                        if rawResult['result']=='ok':
                            u = rawResult['uuid']
                            template_file = open("/".join((PRODUCER_RATE_ROOT, "temp", self.uuid[-12:], u[-12:-10], u[-10:]+".t")), 'rb')
                            template = template_file.read()
                            template_file.close()
                            template = base64.b64encode(template)
                            cache_value.append(template)
                        elif rawResult['result']=='failed':
                            pass
                        self.enrollCallBack_cache_conn.set(formEnrollKey(self.enrollEXEUUID, rawResult['uuid']), json.dumps(cache_value))
                    except Exception, e:
#                        print e
                        continue

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
                if USE_MEMCACHE:
                    cache_value = [rawResult['result'], rawResult['match_type']]
                if rawResult['result'] == 'ok':
                    print>>self.match_result_file, '%s %s %s ok %s' % (bxxid1, bxxid2, rawResult['match_type'], rawResult['score'])
                    if USE_MEMCACHE:
                        cache_value.append(rawResult['score'])
                elif rawResult['result'] == 'failed':
                    print>>self.match_result_file, '%s %s %s failed' % (bxxid1, bxxid2, rawResult['match_type'])
                    self.failed_match_count += 1
                if USE_MEMCACHE:
                    try:
                        cache_key = formMatchKey(self.matchEXEUUID, bxxid1, bxxid2)
                        self.matchCallBack_cache_conn.set(cache_key, json.dumps(cache_value))
                    except Exception, e:
#                        print e
                        pass
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
            if USE_MEMCACHE:
                self.enrollCallBack_cache_conn = getMemcacheConn(self.host)
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
            if USE_MEMCACHE:
                self.matchCallBack_cache_conn = getMemcacheConn(self.host)
            ch.basic_consume(self.matchCallBack, queue=self.match_result_qname)
            print 'match start consuming'
            ch.start_consuming()
            print "match_result_ch consumed"
        ch.queue_delete(queue=self.match_result_qname)

        self.match_result_file.close()

        # matchresult2bxx(self.benchmark_file_dir, self.result_file_dir)
#        conn.close()
