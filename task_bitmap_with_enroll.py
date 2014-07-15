import sys
import struct

def bxx2num(bxx):
  bxx = bxx[::-1]
  base = 93
  num = ord(bxx[0]) - ord('!')
  for c in bxx[1:]:
      num = num * 93 + ord(c) - ord('!')
  return num

class BMManager:
  def __init__(self, bmfname, nsample):
    self.nsample = nsample
    self.bmfname = bmfname
    self.bmf = open(bmfname, 'a')
    self.MATCH_RESULT_LIMIT = self.nsample * (self.nsample + 1) + 10
    self.ENROLL_RESULT_LIMIT = self.MATCH_RESULT_LIMIT + self.nsample + 10
    self.ENROLL_RESULT_START = self.MATCH_RESULT_LIMIT + 5
    self.bmf.seek(self.ENROLL_RESULT_LIMIT)
    self.bmf.write(struct.pack('B', 0))
    self.bmrf = open(bmfname, 'r')

  def get_key(self, bxx1, bxx2):
    num1 = bxx2num(bxx1)
    num2 = bxx2num(bxx2)
    if num1 > num2:
      num1, num2 = num2, num1
    key = (num1-1) * self.nsample + num2

    return key

  def create_bitmap(self, infname, enroll=False):
    inf = open(infname, 'r')

    i = 0
    while True:
      line = inf.readline()
      if line == "" or line == "\n":
        break
      line = line.rstrip("\n")
      self.update_bitmap([line], enroll)
      i += 1
      if i % 100000 == 0:
        print '%i done' % (i)

    inf.close()

  def update_bitmap(self, lines, enroll=False):
    for line in lines:
      key = None
      if not enroll:
        bxx1, bxx2 = line.split(' ')[:2]
        key = self.get_key(bxx1, bxx2)
      else:
        bxx = line.split(' ')[0]
        key = bxx2num(bxx) + self.ENROLL_RESULT_START
      self.bmf.seek(key)
      self.bmf.write(struct.pack('B', 1))

  def query_line(self, line, enroll=False):
    key = None
    if enroll:
      bxx = line.split(' ')[0]
      key = bxx2num(bxx) + self.ENROLL_RESULT_START
    else:
      bxx1, bxx2 = line.split(' ')[:2]
      key = self.get_key(bxx1, bxx2)

    self.bmrf.seek(key)
    done = struct.unpack('B', self.bmrf.read(1))[0]
    return done

  def query(self, bxx1, bxx2):
    key = self.get_key(bxx1, bxx2)
    self.bmrf.seek(key)
    done = struct.unpac('B', self.bmrf.read(1))[0]
    return done

  def destroy(self):
    self.bmf.close()

if __name__ == '__main__':
  if len(sys.argv) < 4:
    print "Usage: python tasl_bitmap.py bxxfile bmfile nsample"
    sys.exit()
  manager = BMManager(sys.argv[2], int(sys.argv[3]))
  manager.create_bitmap(sys.argv[1])
  manager.destroy()
