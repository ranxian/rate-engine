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

config = ConfigParser.ConfigParser()
config.readfp(open('%s/producer.conf' % os.path.dirname(__file__), 'r'))

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

        self.state_file_path = "/".join((result_file_dir, "state.txt"))

#        self.conn = TornadoConnection(pika.ConnectionParameters(host))
        self.conn = pika.BlockingConnection(pika.ConnectionParameters(self.host,2013))
        self.ch = self.conn.channel()
        print "queue server connected"
        self.results = {}
        self.uuid = uuid.uuid4().__str__()
        self.enroll_subtask_uuids = []
        self.match_subtask_uuids = []
        self.finished_enroll_subtask_uuids = []
        self.finished_match_subtask_uuids = []
        if not os.path.isdir(result_file_dir):
            os.makedirs(result_file_dir)
        self.enroll_result_file = open("/".join((result_file_dir, 'enroll_result.txt')), 'w')
        self.match_result_file = open("/".join((result_file_dir, 'match_result.txt')), 'w')
        self.finished_enroll_uuids = set()
#        self.finished_match_uuids = set()
        self.failed_enroll_uuids = set()
        self.failed_match_uuids = set()
        self.submitting_enroll = False
        self.submitting_match = False
        self.enroll_lock = threading.Lock() # should be locked whether submitting or receiving result
        self.match_lock = threading.Lock()
        self.enroll_result_qname = 'results-enroll-%s' % (self.uuid,)
        self.match_result_qname = 'results-match-%s' % (self.uuid,)
        self.enroll_uuids = set()

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

    def prepare(self):
        print "preparing queues"
        self.ch.queue_declare(queue='jobs', durable=False, exclusive=False, auto_delete=False)
        # prepare dirs
        print "preparing dirs on server"
        os.makedirs("/".join((PRODUCER_RATE_ROOT, 'temp', self.uuid[-12:])))
        for i in range(16*16):
            tdir = str(hex(i+256))[-2:]
            os.mkdir("/".join((PRODUCER_RATE_ROOT, 'temp', self.uuid[-12:], tdir)))
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
            enroll_result_thread.start()
        wait = False
        while True:
            print 'While loop'
            if wait == True:
                time.sleep(60)
                wait = False
            print 'Acquiring lock 1'
            with self.enroll_lock:
                print 'Lock get'
                working_enroll_subtasks = len(self.enroll_subtask_uuids)-len(self.finished_enroll_subtask_uuids)
                if working_enroll_subtasks >= MAX_WORKING_ENROLL_SUBTASKS:
                    print "%d enroll queue full, wait for 1 min" % (working_enroll_subtasks, )
                    wait = True
                    continue
                print 'Begin read'
                a = enrollf.readline()
                print 'Line read'
                lines_proceeded += 1
                if lines_proceeded%1000==0:
                    print "%d enrolls proceeded" % lines_proceeded

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

        print 'Acquiring lock 2'
        with self.enroll_lock:
            print 'Lock 2 get'
            if len(l)!=0:
                self.submitEnrollBlock(l)
                l = []
                enrollf.close()
                #enroll_log_file.close()
            if len(self.finished_enroll_subtask_uuids)==len(self.enroll_subtask_uuids):
                print "enroll workers finished before producer reach this line"
                try:
                    self.enroll_result_ch.stop_consuming()
                except Exception, e:
                    print e
            self.submitting_enroll = False

        print "%d enrolls" % len(self.enroll_uuids)
        print "all enrolls submitted, waiting for all results"
        enroll_result_thread.join()
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
                if i%10000==0:
                    print "%d matches proceeded" % (i,)

                a = benchmarkf.readline() # 11 22 I
                if len(a)==0:
                    break
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

        with self.match_lock:
            if len(l)!=0:
                self.submitMatchBlock(l)
                l = []
            if len(self.match_subtask_uuids) == len(self.finished_match_subtask_uuids):
                print "match workers finished before producer reach this line"
                try:
                    self.match_result_ch.stop_consuming()
                except Exception, e:
                    print e
            self.submitting_match = False
        benchmarkf.close()

        print "%d matches" % self.submitted_match_count
        print "all matches submitted, waiting for all results"
        match_result_thread.join()
        print "match finished, failed %d" % self.failed_match_count

    def solve(self):
        try:
            self.prepare()
            self.doEnroll()
            self.doMatch()
    #        self.generateResults()
            self.cleanUp()
        except Exception, e:
            state_file = open(self.state_file_path, 'w')
            state_file.write("1\n")
            state_file.close()
            raise e

    def cleanUp(self):
        print "cleaning up"
        try:
            temp_dir = "/".join((PRODUCER_RATE_ROOT, 'temp', self.uuid[-12:]))
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
        except Exception, e:
            print e

        self.ch.exchange_declare(exchange='jobs-cleanup-exchange', type='fanout')
        cleanup_dir = "/".join(('temp', self.uuid[-12:]))
        self.ch.basic_publish(exchange='jobs-cleanup-exchange', routing_key='', body=pickle.dumps(cleanup_dir))

    def submit(self, subtask):
        if subtask==None:
            return
        for fpath in subtask['files']:
            fpath = "/".join((PRODUCER_RATE_ROOT, fpath))
            if not os.path.exists(fpath):
                raise Exception("file does not exists: %s" % fpath)
        self.ch.basic_publish(exchange='', routing_key='jobs', body=pickle.dumps(subtask))

    def enrollCallBack(self, ch, method, properties, body):
#        print "enrollCallBack called"
        progress_part = 0.3
        with self.enroll_lock:

#            print "enrollCallBack runs"
            result = pickle.loads(body)
            self.finished_enroll_subtask_uuids.append(result['subtask_uuid'])
            ch.basic_ack(delivery_tag=method.delivery_tag)
            for rawResult in result['results']:
                bxxid = self.uuid_bxx_table[rawResult['uuid']]
                self.finished_enroll_uuids.add(bxxid)
                print>>self.enroll_result_file, "%s %s" % (bxxid, rawResult['result'])
                if rawResult['result']=='failed':
                    self.failed_enroll_uuids.add(bxxid)
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
#                print "enrollCallBack stop consuming"
                ch.stop_consuming()

    def matchCallBack(self, ch, method, properties, body):
        with self.match_lock:
            result = pickle.loads(body)
            self.finished_match_subtask_uuids.append(result['subtask_uuid'])
            ch.basic_ack(delivery_tag=method.delivery_tag)
            for rawResult in result['results']:
                bxxid1 = self.uuid_bxx_table[rawResult['uuid1']]
                bxxid2 = self.uuid_bxx_table[rawResult['uuid2']]
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
            print "match result [%s] finished [%d/%d=%d%%] failed [%d/%d]" % (result['subtask_uuid'][:8], len(self.finished_match_subtask_uuids), len(self.match_subtask_uuids), 100*len(self.finished_match_subtask_uuids)/len(self.match_subtask_uuids), self.failed_match_count, self.submitted_match_count)
            state_file = open(self.state_file_path, 'w')
            state_file.write("%f\n" % (0.3 + 0.7 * float(len(self.finished_match_subtask_uuids))/len(self.match_subtask_uuids)))
            state_file.close()

#            self.match_result_file.flush()
            if (not self.submitting_match) and len(self.finished_match_subtask_uuids)==len(self.match_subtask_uuids):
                ch.stop_consuming()

    def waitForEnrollResults(self):
        print 'waiting for enroll results'
        conn = pika.BlockingConnection(pika.ConnectionParameters(self.host, 2013))
        ch = conn.channel()
        self.enroll_result_ch = ch
        ch.queue_declare(queue=self.enroll_result_qname, durable=False, exclusive=False, auto_delete=False)
        if (not self.submitting_enroll) and len(self.finished_enroll_subtask_uuids)==len(self.enroll_subtask_uuids):
            pass
        else:
            if USE_MEMCACHE:
                self.enrollCallBack_cache_conn = getMemcacheConn(self.host)
            ch.basic_consume(self.enrollCallBack, queue=self.enroll_result_qname)
            ch.start_consuming()
        ch.queue_delete(queue=self.enroll_result_qname)
#        conn.close()

    def waitForMatchResults(self):
        print 'waiting for match results'
        conn = pika.BlockingConnection(pika.ConnectionParameters(self.host,2013))
        ch = conn.channel()
        self.match_result_ch = ch
        ch.queue_declare(queue=self.match_result_qname, durable=False, exclusive=False, auto_delete=False)
        if (not self.submitting_match) and len(self.finished_match_subtask_uuids)==len(self.match_subtask_uuids):
            pass
        else:
            if USE_MEMCACHE:
                self.matchCallBack_cache_conn = getMemcacheConn(self.host)
            ch.basic_consume(self.matchCallBack, queue=self.match_result_qname)
            ch.start_consuming()
        ch.queue_delete(queue=self.match_result_qname)

        self.match_result_file.close()
        matchresult2bxx(self.benchmark_file_dir, self.result_file_dir)
#        conn.close()
