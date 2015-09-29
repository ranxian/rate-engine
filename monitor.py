import zipfile
import shutil
import os
import urllib
import urllib2
import json
import time
import json
import ConfigParser
import psutil
from threading import Thread
from multiprocessing import Process
import cherrypy

config = ConfigParser.ConfigParser()
config.readfp(open('worker.conf', 'r'))

WEB_SERVER=config.get('rate-worker', 'WEB_SERVER')

class Monitor(object):
  def shutdown_workers(self):
    try:
      stop_workers()
    except Exception, e:
      print "can't kill workers", e
      raise Exception("can't kill workers")

  def start_workers(self):
    try:
      start_workers()
    except Exception, e:
      print "can't start workers", e
      raise Exception("can't start workers")

  @cherrypy.expose
  def index(self):
    return "This is the monitor of RATE worker"

  @cherrypy.expose
  @cherrypy.tools.json_out()
  def update_task(self, uuids):
    uuids = uuids.split(",")
    with open('task_uuids.txt', 'w') as f:
      for uuid in uuids:
        f.write('%s\n' % (uuid))

    return { 'result': 'ok' }

  @cherrypy.expose
  @cherrypy.tools.json_out()
  def pull_worker(self, worker_url):
    try:
      # First shutdown all workers
      self.shutdown_workers()
      # Then download
      urllib.urlretrieve(worker_url, 'worker-update.zip')
      zf = zipfile.ZipFile('worker-update.zip')
      zf.extractall()
      first_dir = zf.namelist()[0]
      # Then move
      for f in os.listdir(first_dir):
        path = first_dir + f
        shutil.move(path, '.')
      # Then restart
      self.start_workers()

      return { 'result': 'ok' }
    except Exception, e:
      return { 'result': 'fail', 'reason': str(e) }

  @cherrypy.expose
  @cherrypy.tools.json_out()
  def shutdown(self):
    try:
      self.shutdown_workers()
      return { 'result': 'ok' }
    except Exception, e:
      return { 'result': 'fail', 'reason': str(e) }

  @cherrypy.expose
  @cherrypy.tools.json_out()
  def restart(self):
    try:
      self.shutdown_workers()
      self.start_workers()
      return { 'result': 'ok' }
    except Exception, e:
      return { 'result': 'fail', 'reason': str(e) }

  @cherrypy.expose
  @cherrypy.tools.json_out()
  def start(self):
    try:
      self.start_workers()
      return { 'result': 'ok' }
    except Exception, e:
      return { 'result': 'fail', 'reason': str(e) }
      
def start_workers():
  os.system('start "RATE_MONITOR" monitor.bat')
  
def stop_workers():
  os.system('taskkill /F /FI "Windowtitle eq RATE_MONITOR*" /IM cmd.exe /T')
  
def update_task_uuids():
  request = urllib2.Request("http://" + WEB_SERVER + "/admin/task_list")
  result = urllib2.urlopen(request)
  
  body = result.read()
  queues_json = json.loads(body)
  
  with open('task_uuids.txt', 'w') as f:
    for uuid in queues_json['task_uuids']:
      f.write('%s\n' % (uuid))

def make_heartbeat():
  while True:
    try:
      url = 'http://' + WEB_SERVER + '/worker_heartbeat?'
      
      cpupercent = psutil.cpu_percent(interval=1)
      url += 'cpupercent=' + str(cpupercent)

      mem = psutil.virtual_memory()
      url += '&memtotal=' + str(mem.total)
      url += '&memavailable=' + str(mem.available)
      url += '&mempercent=' + str(mem.percent)

      f = urllib2.urlopen(url)
      
      print f.read()
    except Exception, e:
      print e

    time.sleep(5)
    
def start_server():
  cherrypy.config.update({ 'server.socket_host': '0.0.0.0' })
  cherrypy.quickstart(Monitor())

worker_process = None
  
if __name__ == '__main__':
  # Start heartbeat thread
  t = Thread(target=make_heartbeat, args=())
  t.daemon = True
  t.start()
  
  t = Thread(target=start_server, args=())
  t.daemon = True
  t.start()
  
  # Update task uuids
  update_task_uuids()
  start_workers()
  
  try:
    while True:
      time.sleep(5)
  except Exception, e:
    print e
  except KeyboardInterrupt, e:
    print e
  finally:
    stop_workers()
