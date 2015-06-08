import cherrypy
import zipfile
import urllib
import shutil
import os
import urllib2
import json
import time
import json
import psutil
import ConfigParser
from threading import Thread
from subprocess import call

config = ConfigParser.ConfigParser()
config.readfp(open('%s/worker.conf' % os.path.dirname(os.path.realpath(__file__)), 'r'))

WEB_SERVER=config.get('rate-worker', 'WEB_SERVER')

class Monitor(object):
  def shutdown_workers(self):
    try:
      call(['taskkill', '/F', '/IM', 'cmd.exe', '/T'])
      call(['taskkill', '/F', '/IM', 'worker.exe', '/T'])
    except Exception, e:
      print "can't kill workers", e
      raise Exception("can't kill workers")

  def start_workers(self):
    try:
      call(['cmd.exe', 'monitor.bat'])
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

def make_heartbeat():
  import os
import socket

if os.name != "nt":
    import fcntl
    import struct

    def get_interface_ip(ifname):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return socket.inet_ntoa(fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s',
                                ifname[:15]))[20:24])

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

if __name__ == '__main__':
  # Start heartbeat thread
  t = Thread(target=make_heartbeat, args=())
  t.daemon = True
  t.start()

  try:
    while True:
      time.sleep(5)
  except Exception, e:
    print e
  except KeyboardInterrupt, e:
    print e

  # cherrypy.quickstart(Monitor())

