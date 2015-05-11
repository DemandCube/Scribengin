import json

class Descriptor():
  def __init__(self):
    pass
  
  def getDict(self):
    pass
  
  def getJson(self):
    return json.dumps(self.getDict())