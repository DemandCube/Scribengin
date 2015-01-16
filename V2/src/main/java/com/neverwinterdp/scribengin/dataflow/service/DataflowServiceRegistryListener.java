package com.neverwinterdp.scribengin.dataflow.service;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeEvent;
import com.neverwinterdp.registry.NodeWatcher;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.service.ScribenginService;
import com.neverwinterdp.vm.VMDescriptor;

public class DataflowServiceRegistryListener {
  private Logger logger = LoggerFactory.getLogger(DataflowServiceRegistryListener.class) ;
  
  private Registry registry ;
  private List<LeaderListener> leaderListeners = new ArrayList<>();
  
  public DataflowServiceRegistryListener(Registry registry) throws RegistryException {
    this.registry = registry;
    registerLeaderWatcher();
  }
  
  public void add(LeaderListener listener) {
    leaderListeners.add(listener);
  }
  
  public void close() { registry = null ; }
  
  public boolean isClosed() { return registry == null ; }

  private void registerLeaderWatcher() throws RegistryException {
    if(registry.exists(ScribenginService.LEADER_PATH)) {
      registry.watchModify(ScribenginService.LEADER_PATH, new LeaderNodeWatcher());
    } else {
      registry.watchExists(ScribenginService.LEADER_PATH, new NodeWatcher() {
        @Override
        public void process(NodeEvent event) {
          if(event.getType() == NodeEvent.Type.CREATE) {
            try {
              registry.watchModify(ScribenginService.LEADER_PATH, new LeaderNodeWatcher());
            } catch (RegistryException e) {
              logger.error("Cannot register the leader node watcher", e);
            }
          }
        }
      });
    }
  }
  
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
