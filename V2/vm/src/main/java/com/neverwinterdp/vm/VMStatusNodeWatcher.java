package com.neverwinterdp.vm;

import com.neverwinterdp.registry.NodeEvent;
import com.neverwinterdp.registry.NodeWatcher;
import com.neverwinterdp.registry.Registry;

public class VMStatusNodeWatcher extends NodeWatcher {
  private Registry registry;
  
  public VMStatusNodeWatcher(Registry registry) {
    this.registry = registry;
  }
  
  @Override
  public void process(NodeEvent event) {
    //System.out.println("got status event " + event.getPath());
    try {
      if(event.getType() == NodeEvent.Type.MODIFY || event.getType() == NodeEvent.Type.CREATE) {
        String path = event.getPath();
        String descriptorPath = path.substring(0, path.lastIndexOf('/')) ;
        VMDescriptor vmDescriptor = registry.getDataAs(descriptorPath, VMDescriptor.class);
        VMStatus vmStatus = registry.getDataAs(path, VMStatus.class);
        onChange(event, vmDescriptor, vmStatus);
      } 
    } catch(Exception ex) {
      ex.printStackTrace();
    }
  }
  
  public void onChange(NodeEvent event, VMDescriptor vmDescriptor, VMStatus vmStatus) throws Exception {
    throw new Exception("This method need to override!") ;
  }
}
