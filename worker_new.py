import ConfigParser
import multiprocessing
import traceback
import os
import time
import pika
import socket
from multiprocessing import Process
from pika.exceptions import AMQPConnectionError


config = ConfigParser.ConfigParser()
config.readfp(open('worker.conf', 'r'))
SERVER=config.get('rate-worker', 'SERVER')
WORKER_NUM=config.getint('rate-worker', 'WORKER_NUM')
WORKER_RATE_ROOT=config.get('rate-worker', 'WORKER_RATE_ROOT')

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
        self.CURRENT_WORKER_NUM = CURRENT_WORKER_NUM

        self.download_ftp = None

        process_lock.acquire()
        self.worker_num = CURRENT_WORKER_NUM.value
        CURRENT_WORKER_NUM.value = CURRENT_WORKER_NUM.value + 1
        process_lock.release()

        self.download_ftp = None
        self.conn = None

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

    def doWork(self):
        pass

    def solve(self):
        while True:
            try:
                self.conn = pika.BlockingConnection(pika.ConnectionParameters(self.host))
                self.ch = self.conn.channel()
                print "[%d] [%s]" % (os.getpid(), str(self.worker_num)), 'queue server connected'
                self.ch.basic_qos(prefetch_count=1)

                self.ch.exchange_declare(exchange='jobs-cleanup-exchange', type='fanout')
                my_cleanup_queue_name = self.ch.queue_declare(exclusive=True).method.queue
                self.ch.queue_bind(exchange='jobs-cleanup-exchange', queue=my_cleanup_queue_name)
                self.ch.basic_consume(self.doClean, queue=my_cleanup_queue_name, no_ack=True)

                self.ch.queue_declare(queue = 'jobs', durable=False, exclusive=False, auto_delete=False)
                self.ch.queue_bind
                self.ch.basic_consume(self.doWork, queue='jobs')

                self.ch.start_consuming()
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
            time.sleep(1)

def clean_tmp_files():
    while True:
        try:
            os.system("del *.tmp")
            time.sleep(600)
        except Exception, e:
            pass

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
