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
    return json.loads(requests.get(url, params=payload).text)
  
  def getApplicationMasters(self):
    return self.getRequest("ws/v1/cluster/apps")
  
  def getApplicationMasterInfo(self, applicationMasterId):
    return self.getRequest("ws/v1/cluster/apps/"+applicationMasterId)
  
  def getRunningApplicationMasters(self):
    return self.getRequest("ws/v1/cluster/apps", {"state":"RUNNING"}) 
  
  #Returns an array of applicationIds
  def getRunningApplicationMasterIds(self):
    ids = ""
    try:
      for appMaster in self.getRunningApplicationMasters()["apps"]["app"]:
        if "id" in appMaster:
          ids = appMaster["id"]+","+ids
    except:
      pass
    #Remove last element if its empty
    ids = ids.split(",")
    if ids[-1] == "":
      del ids[-1]
    return ids
  
  def getNumContainersForApplicationMaster(self, applicationMasterId):
    return self.getRequest("ws/v1/cluster/apps/"+applicationMasterId)["app"]["runningContainers"]