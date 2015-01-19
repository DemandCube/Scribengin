package com.neverwinterdp.registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegistryNodeEventListener {
  protected Logger logger = LoggerFactory.getLogger(RegistryNodeEventListener.class) ;
  
  protected Registry registry ; 
  
  public RegistryNodeEventListener(Registry registry) {
    this.registry = registry;
  }
  
  public void close() {
    registry = null ;
  }
  
  public boolean isClosed() {
    return registry == null || !registry.isConnect();
  }
  
  
  protected void watch(final String listenPath, final NodeWatcher nodeWatcher) throws RegistryException {
    if(registry.exists(listenPath)) {
      registry.watchModify(listenPath, nodeWatcher);
    } else {
      registry.watchExists(listenPath, new NodeWatcher() {
        @Override
        public void process(NodeEvent event) {
          if(event.getType() == NodeEvent.Type.CREATE) {
            try {
              registry.watchModify(listenPath, nodeWatcher);
              nodeWatcher.process(event);
            } catch (RegistryException e) {
              logger.error("Cannot register the leader node watcher", e);
            }
          }
        }
      });
    }
  }

}
