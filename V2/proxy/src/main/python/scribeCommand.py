import requests, click, json, logging, sys

#r = requests.get("http://www.github.com/DemandCube/Scribengin")
#print r.content
_debug = False
_host = ''
_logfile = ''


@click.group()
@click.option('--debug/--no-debug', default=False, help="Turn debugging on")
@click.option('--host', default='http://127.0.0.1:8181', help="Host to connect to")
@click.option('--logfile', default='scribeCommand.log', help="Log file to write to")
def mastercommand(debug, host, logfile):
  global _debug, _host, _logfile
  _debug = debug
  _host = host
  _logfile = logfile
  if _debug:
      #Set logging file, overwrite file, set logging level to DEBUG
      logging.basicConfig(filename=_logfile, filemode="w", level=logging.DEBUG)
      logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
      click.echo('Debug mode is %s' % ('on' if debug else 'off'))
  else:
    #Set logging file, overwrite file, set logging level to INFO
    logging.basicConfig(filename=_logfile, filemode="w", level=logging.INFO)



@mastercommand.command(help="- List all running VMs")
def listvms():
  logging.debug("Listing VMs")
  payload = {'command': 'vm list'}
  res = sendRequest(payload)
  click.echo(res)

@mastercommand.command(help="- Dump the registry")
@click.option('--path', default='/', help="Path to dump")
def registrydump(path):
  logging.debug("Dump Registry")
  payload = {
    'command': 'registry dump',
    'path' : path,
  }
  res = sendRequest(payload)
  click.echo(res)

@mastercommand.command(help="- Not implemented yet :)")
def scribenginmaster():
  logging.debug("Scribengin master")
  payload = {'command': 'scribengin master'}
  res = sendRequest(payload)
  click.echo(res)

@mastercommand.command(help="- Submit a new dataflow")
@click.option('--jsonfile', type=click.File('r'), default='dataflow.json', help="Path to file containing json for dataflow config")
def submitdataflow(jsonfile):
  logging.debug("Submit New Dataflow")
  payload = json.load(jsonfile)
  payload["command"] = "dataflow"
  res = sendRequest(payload)
  click.echo(res)


def sendRequest(params):
  global _host
  logging.debug("Sending Request")
  logging.debug("HOST: " + _host)
  logging.debug("PARAMETERS: " + str(params))
    
  try:
    r = requests.post(_host, data=params)
  except Exception, e:
    return "Error handling request: "+str(e)
  return r.text


if __name__ == '__main__':
  #x = {
  #      "command": "dataflow",
  #      "dataflow-Name"                   :"dataflowName",
  #      "dataflow-Dataprocessor"          : "dataflowDataprocessor",
  #      "dataflow-NumWorkers"             : "10",
  #      "dataflow-NumExecutorsPerWorkers" : "20",
  #      "source-Type"       : "KAFKA",
  #      "source-Name"       : "sourceName",
  #      "source-Topic"      : "sourceTopic",
  #      "source-ZkConnect"  : "sourceZkConnect",
  #      "source-BrokerList" : "sourceBrokerList",
  #      "sink-Type"       : "KAFKA",
  #      "sink-Name"       : "sinkName",
  #      "sink-Topic"      : "sinkTopic",
  #      "sink-ZkConnect"  : "sinkZkConnect",
  #      "sink-BrokerList" : "sinkBrokerList",
  #      "invalidsink-Type"       : "KAFKA",
  #      "invalidsink-Name"       : "invalidsinkName",
  #      "invalidsink-Topic"      : "invalidsinkTopic",
  #      "invalidsink-ZkConnect"  : "invalidsinkZkConnect",
  #      "invalidsink-BrokerList" : "invalidsinkBrokerList",
  #}
  #filex = open("newfile.json", "w+")
  #print json.dump(x, filex)
  #
  #filex.close
  mastercommand()