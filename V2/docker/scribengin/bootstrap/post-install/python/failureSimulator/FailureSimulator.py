#! /bin/sh
""":"
exec python $0 ${1+"$@"}
"""


#from ScribeServer import ScribeServer
from ScribeServer import ScribeServer
from ScribeProcess import ScribeProcess
ss = ScribeServer()
sp = ScribeProcess()

#a = ss.execServer("ls -la /opt", "kafka")

#for b in a:
#  print b+": "
#  print a[b]["stdout"]
  
  
print sp.getRunningKafkaProcessIDs()
print sp.getRunningZookeeperProcessIDs()