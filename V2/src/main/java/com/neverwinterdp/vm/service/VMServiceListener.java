package com.neverwinterdp.vm.service;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeEvent;
import com.neverwinterdp.registry.NodeWatcher;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.vm.VMDescriptor;

public class VMServiceListener {
  private Registry registry;
  private List<LeaderListener> leaderListeners = new ArrayList<>();
  
  public VMServiceListener(Registry registry) throws RegistryException {
    this.registry = registry;
    registry.watchModify(VMService.LEADER_PATH, new LeaderNodeWatcher());
  }
  
  public void add(LeaderListener listener) {
    leaderListeners.add(listener);
  }
  
  public void close() { registry = null ; }
  
  public boolean isClosed() { return registry == null ; }

  public class LeaderNodeWatcher implements NodeWatcher {
    @Override
    public void process(NodeEvent event) {
      if(isClosed()) return;
      try {
        String path = event.getPath();
        if(event.getType() == NodeEvent.Type.MODIFY) {
          registry.watchModify(path, this);
          Node node = registry.getRef(path);
          VMDescriptor vmDescriptor = node.getData(VMDescriptor.class);
          for(LeaderListener sel : leaderListeners) {
            sel.onElected(event, vmDescriptor);
          }
        }
      } catch(Exception ex) {
        ex.printStackTrace();
      }
    }
  }
  
  static public interface LeaderListener {
    public void onElected(NodeEvent event, VMDescriptor vmDescriptor) ;
  }
}
