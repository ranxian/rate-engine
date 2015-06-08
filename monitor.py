import cherrypy
import zipfile
import urllib
import shutil
import os
from subprocess import call

class Monitor(object):
  def shutdown_workers(self):
    call(['taskkill', '/F', '/IM', '/T', 'worker.exe'])

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
    # First shutdown all workers
    self.shutdown_workers()

    urllib.urlretrieve(worker_url, 'worker-update.zip')
    zf = zipfile.ZipFile('worker-update.zip')
    zf.extractall()
    first_dir = zf.namelist()[0]
    for f in os.listdir(first_dir):
      path = first_dir + f
      shutil.move(path, '.')
    
    return { 'result': 'ok' }

  @cherrypy.expose
  def shutdown(self):
    pass

  @cherrypy.expose
  def restart(self):
    pass

  @cherrypy.expose
  def start(self):
    pass

if __name__ == '__main__':
  cherrypy.quickstart(Monitor())
  