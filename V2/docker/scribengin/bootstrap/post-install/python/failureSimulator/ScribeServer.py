import re, paramiko
from os.path import expanduser, join

class ScribeServer:
  """
  A class to help discover, store, sort, and communicate with Scribengin Servers
  """
  
  """
  Object to store IP/hostname info of Scribengin Servers
  """
  servers = {}

  
  """
  Regexes to find servers from /etc/hosts
  """
  serverRegexes = [
    re.compile('.*kafka-\d+.*'),
    re.compile('.*zookeeper-\d+.*'),
    re.compile('.*hadoop-worker-\d+.*'),
    re.compile('.*hadoop-master-\d+.*'),
  ]
  
  def __init__(self, etcHostsPath="/etc/hosts", defaultSSHKeyPath=join(expanduser("~"),".ssh/id_rsa")):
    """
    Initialize and immediately parse the /etc/hosts file
    """
    self.parseEtcHosts(etcHostsPath)
    self.defaultSSHKeyPath = defaultSSHKeyPath
      
  
  def parseEtcHosts(self, path="/etc/hosts"):
    """
    Parses the file in path (/etc/hosts) and searches for pattern of [ip address]\s+[hostname]
    Uses serverRegexes for server name regexes
    """
    f = open(path, 'r')
    for line in f:
      if any(regex.match(line) for regex in self.serverRegexes):
        self.servers[line.rstrip().split()[1]] = line.rstrip().split()[0]
    
  
  
  def execServer(self, command, serverRegex=".*"):
    """
    Executes a command on a server.  serverRegex is ".*" by default, and thus will execute on all scribengin servers
    If you execute execServer("kafka") it will only execute on host names that match "*kafka*"
    Returns a hash in format of:
    { servername1: {stdout: "Command's stdout output",
                  stderr: "Command's stderr output"}
      servername2: {stdout: "Command's stdout output",
                  stderr: "Command's stderr output"} }
    """
    results = {}
    for server in self.servers:
      if re.compile(serverRegex).match(server):
        stdout,stderr = self.sshExecute(server, command)
        results[server]= {"stdout": stdout,
                          "stderr": stderr}
    return results
  
  
  def sshExecute(self, host, command, username = "neverwinterdp"):
    """
    SSH onto a machine, execute a command
    Returns stdout,stderr
    """
    key = paramiko.RSAKey.from_private_key_file(self.defaultSSHKeyPath)
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect( hostname = host, username = username, pkey = key )
    stdin , stdout, stderr = c.exec_command(command)
    
    stdout = stdout.read()
    stderr = stderr.read()
    c.close()
    
    return stdout,stderr
  