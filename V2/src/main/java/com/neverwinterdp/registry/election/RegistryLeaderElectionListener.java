package com.neverwinterdp.registry.election;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeEvent;
import com.neverwinterdp.registry.NodeWatcher;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;

public class RegistryLeaderElectionListener<D> {
  protected Logger logger = LoggerFactory.getLogger(RegistryLeaderElectionListener.class) ;
  
  private Class<D> descriptorType ;
  private String   listenPath;
  private Registry registry ;
  
  
  public RegistryLeaderElectionListener(Registry registry, Class<D> descriptorType, String listenPath) throws RegistryException {
    this.registry = registry;
    this.descriptorType = descriptorType;
    this.listenPath = listenPath;
    registerLeaderWatcher();
  }
  
  private List<LeaderListener<D>> leaderListeners = new ArrayList<>();
  
  
  public void add(LeaderListener<D> listener) {
    leaderListeners.add(listener);
  }
  
  void registerLeaderWatcher() throws RegistryException {
    if(registry.exists(listenPath)) {
      registry.watchModify(listenPath, new LeaderNodeWatcher());
    } else {
      registry.watchExists(listenPath, new NodeWatcher() {
        @Override
        public void process(NodeEvent event) {
          if(event.getType() == NodeEvent.Type.CREATE) {
            try {
              registry.watchModify(listenPath, new LeaderNodeWatcher());
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
      if(!registry.isConnect()) return;
      try {
        String path = event.getPath();
        if(event.getType() == NodeEvent.Type.MODIFY) {
          registry.watchModify(path, this);
          Node node = registry.getRef(path);
          D vmDescriptor = node.getData(descriptorType);
          for(LeaderListener<D> sel : leaderListeners) {
            sel.onElected(event, vmDescriptor);
          }
        }
      } catch(Exception ex) {
        ex.printStackTrace();
      }
    }
  }
  
  static public interface LeaderListener<D> {
    public void onElected(NodeEvent event, D descriptor) ;
  }
}
