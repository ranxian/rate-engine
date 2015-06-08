import cherrypy
import zipfile
import urllib
import shutil
import os
from subprocess import call

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

if __name__ == '__main__':
  cherrypy.quickstart(Monitor())
  