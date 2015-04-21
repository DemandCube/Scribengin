#Rebuild images if a file from the last check in contains the string "docker" (i.e. the docker folder was updated)
import subprocess,re
p = subprocess.Popen(['git', 'diff', '--name-only', 'HEAD~1', 'HEAD'], stdout=subprocess.PIPE,  stderr=subprocess.PIPE)
out, err = p.communicate()
rebuild=False
for filename in out.split():
  if re.search( "docker" ,filename):
    rebuild=True

if rebuild:
  p = subprocess.Popen(['./V2/docker/scribengin/docker.sh', 'cluster','--clean-containers','--clean-image', '--build-image'], stdout=subprocess.PIPE,  stderr=subprocess.PIPE)
  out, err = p.communicate()
  print "STDERR: "+err
  print "STDOUT: "+out
  