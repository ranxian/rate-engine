# coding:utf-8
# RATE-engine 消费者

import ConfigParser
import multiprocessing
import traceback
import os
import time
import pika
import socket
import base64
import json
import random
import pickle
import ftplib
import logging
import urllib2
from multiprocessing import Process
from pika.exceptions import AMQPConnectionError

logging.basicConfig()
config = ConfigParser.ConfigParser()
config.readfp(open('worker.conf', 'r'))

SERVER=config.get('rate-worker', 'SERVER')
WORKER_NUM=config.getint('rate-worker', 'WORKER_NUM')
WORKER_RATE_ROOT=config.get('rate-worker', 'WORKER_RATE_ROOT')
CHANGE_QUEUE_COUNT = 100

try:
    FTP_USER=config.get('rate-worker', 'FTP_USER')
except:
    FTP_USER='rate'

try:
    FTP_PASSWORD=config.get('rate-worker', 'FTP_PASSWORD')
except:
    FTP_PASSWORD='xxxxxxxxxxxx'

class Worker:
    def __init__(self, host, file_lock, dir_lock, ftp_mkd_lock, clean_lock, semaphore, process_lock, CURRENT_WORKER_NUM):
        self.host = host

        self.file_lock = file_lock
        self.dir_lock = dir_lock
        self.ftp_mkd_lock = ftp_mkd_lock
        self.clean_lock = clean_lock
        self.semaphore = semaphore
        self.process_lock = process_lock

        self.download_ftp = None
        self.conn = None

        process_lock.acquire()
        self.worker_num = CURRENT_WORKER_NUM.value
        CURRENT_WORKER_NUM.value = CURRENT_WORKER_NUM.value + 1
        process_lock.release()

        self.job_queues = []

    def doClean(self, ch, method, properties, body):
        cleanpath = pickle.loads(body)
        cleanpath = os.path.join(WORKER_RATE_ROOT, cleanpath)
        if not os.path.exists(cleanpath):
            return

        try:
            self.clean_lock.acquire()
            if not os.path.exists(cleanpath):
                return
            print "%s: clean: %s" % (str(self.worker_num), cleanpath)
            shutil.rmtree(cleanpath)
        except Exception, e:
            print e
            traceback.print_exc()
        finally:
            self.clean_lock.release()

    def checkDir(self, dirPath):
        if os.path.isdir(dirPath):
            return

        self.dir_lock.acquire()
        try:
            if not os.path.isdir(dirPath):
                os.makedirs(dirPath)
        except exceptions.OSError, e:
            print e
            traceback.print_exc()
        finally:
            self.dir_lock.release()

    def checkFile(self, relPath):
        tried = 0
        absPath = os.path.join(WORKER_RATE_ROOT, relPath)
        absPath = absPath.replace('\\', '/')
        while not os.path.exists(absPath) or os.stat(absPath).st_size==0:
            self.file_lock.acquire()
            try:
                if os.path.exists(absPath):
                    return
                try:
                    self.checkDir(os.path.dirname(absPath))
                    lf = open(absPath, 'wb')
                    if not self.download_ftp:
                        self.openDownloadFTP()
                    self.download_ftp.retrbinary("RETR " + relPath, lf.write)
                except Exception, e:
                    print(e)
                    if tried > 16:
                        break
                    tried = tried + 1
                    print("%d: download: retry %d times" % (self.worker_num, tried))
                    if lf:
                        lf.close()
                    if os.path.exists(absPath):
                        os.remove(absPath)
                    self.openDownloadFTP() # open ftp only if we need it, and close it when self.prepare() is done
            finally:
                self.file_lock.release()

    def openUploadFTP(self, subtask):
        print "%d: openUploadFTP()" % self.worker_num
        while True:
            try:
                ftp = ftplib.FTP(SERVER, FTP_USER, FTP_PASSWORD)
                ftpdir = 'RATE_ROOT/tasks/%s/templates' % subtask['task_uuid']
                ftp.cwd(ftpdir)
                return ftp
            except Exception, e:
                print e
                traceback.print_exc()
                time.sleep(1)

    def openDownloadFTP(self):
        print "%d: openDownloadFTP()" % self.worker_num
        while True:
            try:
                self.download_ftp = ftplib.FTP(SERVER, FTP_USER, FTP_PASSWORD)
                self.download_ftp.cwd('RATE_ROOT')
                break
            except Exception, e:
                print e
                time.sleep(1)

    def prepare(self, subtask):
        print "%s: prepare" % str(self.worker_num)
        for f in subtask['files']:
            self.checkFile(f)
        if self.download_ftp != None:
            try:
                self.download_ftp.quit()
                self.download_ftp = None
            except Exception, e:
                print e
        print "%s: prepare finished" % str(self.worker_num)

    def doEnroll(self, subtask):
        import rate_run
        enrollEXE = os.path.join(WORKER_RATE_ROOT, subtask['enrollEXE'])
        timelimit = subtask['timelimit']
        memlimit = subtask['memlimit']
        rawResults = []
        ftp = self.openUploadFTP(subtask)
        for tinytask in subtask['tinytasks']:
            u = tinytask['uuid']
            rawResult = { 'uuid': u,'result':'failed' }
            f = tinytask['file']
            self.checkFile(f)
            absImagePath = os.path.join(WORKER_RATE_ROOT, f).replace('/', os.path.sep)
            absTemplatePath = os.path.join(WORKER_RATE_ROOT,'temp',subtask['producer_uuid'][-12:],u[-12:-10], "%s.t" % u[-10:]).replace('/', os.path.sep)
            self.checkDir(os.path.dirname(absTemplatePath))
            cmd = '%s %s %s' % (enrollEXE, absImagePath, absTemplatePath)
            rawResult['result'] = 'ok'
            try:
                start = time.clock()
                (returncode, output) = rate_run.rate_run_main(int(3000), int(memlimit), cmd)
                elapsed = int(100 * (time.clock() - start))
                if returncode == 0 and os.path.exists(absTemplatePath):
                    template_file = open(absTemplatePath, 'rb')
                    tried = 0
                    while True:
                        try:
                            ftp.storbinary('STOR ' + "%s.t" % (tinytask['uuid']), template_file)
                            print 'store', tinytask['uuid'], '.t from', absTemplatePath
                            break
                        except Exception, e:
                            print e
                            traceback.print_exc()
                            print "%d: upload: retry %d" % (self.worker_num, tried)
                            tried = tried + 1
                            ftp = self.openUploadFTP(subtask)
                            if tried == 16:
                                break
                    template_file.close()
                    rawResult['result'] = 'ok'
                    rawResult['time'] = str(elapsed)
                else:
                    print 'did not exit with 0 or did not create template file'
                    rawResult['result'] = 'failed'
            except Exception, e:
                print e
                traceback.print_exc()
                rawResult['result'] = 'failed'
            rawResults.append(rawResult)
        result = {}
        result['results'] = rawResults

        try:
            ftp.quit()
        except Exception, e:
            print e

        return result

    def doMatch(self, subtask):
        import rate_run
        rawResults = []
        timelimit = subtask['timelimit']
        memlimit = subtask['memlimit']
        matchEXE = os.path.join(WORKER_RATE_ROOT, subtask['matchEXE']).replace('/', os.path.sep)

        block_no = subtask['block_no']
        for tinytask in subtask['tinytasks']:
            u1 = tinytask['uuid1']
            u2 = tinytask['uuid2']
            f1 = tinytask['file1']#.replace('/', os.path.sep)
            f2 = tinytask['file2']#.replace('/', os.path.sep)
            f1 = "/".join((WORKER_RATE_ROOT, f1))
            f2 = "/".join((WORKER_RATE_ROOT, f2))
            rawResult = {}
            rawResult['uuid1'] = u1
            rawResult['uuid2'] = u2
            rawResult['match_type'] = tinytask['match_type']

            cmd = '%s %s %s' % (matchEXE, f1, f2)

            try:
                start = time.clock()
                (returncode, output) = rate_run.rate_run_main(int(500), int(memlimit), str(cmd))
                elapsed = int(100 * (time.clock() - start))
                if returncode != 0:
                    rawResult['result'] = 'failed'
                else:
                    score = output.strip()
                    score = float(score)
                    rawResult['result'] = 'ok'
                    rawResult['score'] = str(score)
                    rawResult['time'] = str(elapsed)
            except Exception, e:
                print e
                traceback.print_exc()
                rawResult['result'] = 'failed'
            rawResults.append(rawResult)

        result = {}
        result['results'] = rawResults
        return result

    def doWork(self, method, properties, body):
        subtask = pickle.loads(body)
        self.prepare(subtask)

        try:
            self.semaphore.acquire()
            # do the job
            if subtask['type'] == 'enroll':
                print "%s: enroll begin" % str(self.worker_num)
                result = self.doEnroll(subtask)
                print "%s: enroll finished" % str(self.worker_num)
            elif subtask['type'] == 'match':
                print "%s: match begin" % str(self.worker_num)
                result = self.doMatch(subtask)
                print "%s: match finished" % str(self.worker_num)

            # put result back
            result['block_no'] = subtask['block_no']
            result['type'] = subtask['type']
            result_queue = 'results-%s-%s' % (subtask['type'], subtask['producer_uuid'])
            self.ch.queue_declare(queue=result_queue, durable=False, exclusive=False, auto_delete=False)
            self.ch.basic_publish(exchange='', routing_key=result_queue, body=pickle.dumps(result))
            self.ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception, e:
            traceback.print_exc()
            raise
        finally:
            self.semaphore.release()
    
    def read_job_queues(self):
        queues = []
        while not os.path.isfile('task_uuids.txt'):
            time.sleep(1)
            
        with open('task_uuids.txt', 'r') as f:
            for line in f.readlines():
                uuid = line.rstrip('\n')
                queues.append('jobs-' + uuid)
                
        return queues

    def solve(self):
        self.conn = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        self.ch = self.conn.channel()
        print "[%d] [%s]" % (os.getpid(), str(self.worker_num)), 'queue server connected'
        self.ch.basic_qos(prefetch_count=1)

        self.ch.exchange_declare(exchange='jobs-cleanup-exchange', type='fanout')
        my_cleanup_queue_name = self.ch.queue_declare(exclusive=True).method.queue
        self.ch.queue_bind(exchange='jobs-cleanup-exchange', queue=my_cleanup_queue_name)
        self.ch.basic_consume(self.doClean, queue=my_cleanup_queue_name, no_ack=True)
        
        work_count = 1
        try:
            while True:
                job_queues = self.read_job_queues()
                if len(job_queues) == 0:
                    time.sleep(1)
                    continue

                queue = random.choice(job_queues)
                self.ch.queue_declare(queue = queue, durable=False, exclusive=False, auto_delete=False)
                while work_count % 256 != 0:
                    work_count += 1
                    (method, properties, body) = self.ch.basic_get(queue=queue)
                    if method == None:
                        break
                    self.doWork(method, properties, body)
                work_count = 1
        except AMQPConnectionError, e:
            print 'AMQPConnectionError: ', e
            pass
        except socket.error, e:
            print "socket error: [", os.getpid(),"]", e
        except Exception, e:
            print e
        finally:
            if self.conn:
                self.conn.close()

def clean_tmp_files():
    while True:
        try:
            os.system("del *.tmp")
            time.sleep(600)
        except Exception, e:
            pass

def update_job_queues():
    while True:
        time.sleep(3)
        try:
            request = urllib2.Request("http://rate.pku.edu.cn/admin/task_list")
            result = urllib2.urlopen(request)

            body = result.read()
            queues_json = json.loads(body)

            with open('task_uuids.txt', 'w') as f:
                for uuid in queues_json['task_uuids']:
                    f.write('%s\n' % (uuid))
        except Exception, e:
            print e
            
            
def proc(file_lock, dir_lock, ftp_mkd_lock, clean_lock, semaphore, process_lock, CURRENT_WORKER_NUM):
    while True:
        try:
            w = Worker('%s' % (SERVER, ), file_lock, dir_lock, ftp_mkd_lock, clean_lock, semaphore, process_lock, CURRENT_WORKER_NUM)
            w.solve()
        except Exception, e:
            print e
            traceback.print_exc()
            pass

if __name__=='__main__':
    multiprocessing.freeze_support()

    file_lock = multiprocessing.Lock()
    dir_lock = multiprocessing.Lock()
    ftp_mkd_lock = multiprocessing.Lock()
    clean_lock = multiprocessing.Lock()
    semaphore = multiprocessing.Semaphore(WORKER_NUM)
    process_lock = multiprocessing.Lock()
    CURRENT_WORKER_NUM=multiprocessing.Value("i")
    CURRENT_WORKER_NUM.value = 1

    process_args = []
    process_args.append(file_lock)
    process_args.append(dir_lock)
    process_args.append(ftp_mkd_lock)
    process_args.append(clean_lock)
    process_args.append(semaphore)
    process_args.append(process_lock)
    process_args.append(CURRENT_WORKER_NUM)

    ts = []
    t = Process(target=clean_tmp_files)
    t.daemon = True
    t.start()
    ts.append(t)
    
    # Update task queues
    t = Process(target=update_job_queues, args=())
    t.daemon = True
    t.start()
    ts.append(t)
    
    try:
        for i in range(WORKER_NUM*2):
            t = Process(target=proc, args=process_args)
            t.daemon = True
            t.start()
            ts.append(t)
        while True:
            time.sleep(5)
    except Exception, e:
        print e
    except KeyboardInterrupt, e:
        pass
