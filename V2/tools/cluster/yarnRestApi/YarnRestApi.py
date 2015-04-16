import requests, json, logging


#API at https://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-site/WebServicesIntro.html
class YarnRestApi():
  def __init__(self, hostname, port=8088):
    self.hostname = hostname
    self.port = port
  
  #Returns dictionary of parsed JSON data
  def getRequest(self, path, payload={}):
    url = "http://"+self.hostname+":"+str(self.port)+"/"+path
    logging.debug("Requesting url: "+url)
    try:
      return json.loads(requests.get(url, params=payload).text)
    except:
      return ""
  
  def getApplicationMasters(self):
    return self.getRequest("ws/v1/cluster/apps")
  
  def getApplicationMasterInfo(self, applicationMasterId):
    return self.getRequest("ws/v1/cluster/apps/"+applicationMasterId)
  
  def getRunningApplicationMasters(self):
    return self.getRequest("ws/v1/cluster/apps", {"state":"RUNNING"}) 
  
  def getNodesInfo(self):
    return self.getRequest("ws/v1/cluster/nodes")
  
  def getNodesRunningContainers(self):
    nodes = self.getNodesInfo()
    workers = []
    if nodes and nodes["nodes"] and nodes["nodes"]["node"]:
      for node in nodes["nodes"]["node"] :
        #API to talk to nodemanager talks to node itself
        y = YarnRestApi(node["nodeHTTPAddress"].split(":")[0], node["nodeHTTPAddress"].split(":")[1])
        nodeInfo = y.getRequest("ws/v1/node/apps")
        if nodeInfo and nodeInfo["apps"] and nodeInfo["apps"]["app"] and nodeInfo["apps"]["app"][0]:
          if "containerids" in nodeInfo["apps"]["app"][0]:
            workers.append( node["nodeHTTPAddress"].split(":")[0] )
    return workers
  
  #Returns an array of applicationIds
  def getRunningApplicationMasterIds(self):
    ids = ""
    try:
      runningAppMaster = self.getRunningApplicationMasters()
      if runningAppMaster is not None:
        for appMaster in runningAppMaster["apps"]["app"]:
          if "id" in appMaster:
            ids = appMaster["id"]+","+ids
    except:
      return []
    #Remove last element if its empty
    ids = ids.split(",")
    if ids[-1] == "":
      del ids[-1]
    return ids
  
  def getNumContainersForApplicationMaster(self, applicationMasterId):
    return self.getRequest("ws/v1/cluster/apps/"+applicationMasterId)["app"]["runningContainers"]