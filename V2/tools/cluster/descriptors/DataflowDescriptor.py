from Descriptor import Descriptor

#TODO: Make this better :)
class DataflowDescriptor(Descriptor):

  def __init__(self, ID, name, dataflowAppHome, storageDescriptor, sinkDescriptor, scribe,
               numberOfWorkers=1, numberOfExecutorsPerWorker=1, taskMaxExecuteTime=-1):
    self.descriptor = {
      'id': ID,
      'name': name,
      'dataflowAppHome': dataflowAppHome,
      'storageDescriptor': storageDescriptor,
      'sinkDescriptors': sinkDescriptor,
      'numberOfWorkers': numberOfWorkers,
      'numberOfExecutorsPerWorker': numberOfExecutorsPerWorker,
      'taskMaxExecuteTime': taskMaxExecuteTime,
      'scribe': scribe,
    }
  
  def getDict(self):
    return self.descriptor