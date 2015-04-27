#Rebuild images if a file from the last check in contains the string "docker" (i.e. the docker folder was updated)
import subprocess,re, os
p = subprocess.Popen(['git', 'diff', '--name-only', 'HEAD~1', 'HEAD'], stdout=subprocess.PIPE,  stderr=subprocess.PIPE)
out, err = p.communicate()
rebuild=False
for filename in out.split():
  if re.search( "docker" ,filename):
    rebuild=True

if rebuild:
  os.chdir(os.path.dirname(os.path.realpath(__file__)))
  p = subprocess.Popen(['./docker.sh', 'cluster','--clean-containers','--clean-image', '--build-image'], stdout=subprocess.PIPE,  stderr=subprocess.PIPE)
  out, err = p.communicate()
  print "STDERR: "+err
  print "STDOUT: "+out
else:
  print "No rebuild required"
  