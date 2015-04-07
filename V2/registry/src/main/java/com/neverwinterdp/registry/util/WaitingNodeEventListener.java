package com.neverwinterdp.registry.util;

import java.util.LinkedList;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeWatcher;
import com.neverwinterdp.registry.event.RegistryListener;

public class WaitingNodeEventListener {
  private RegistryListener registryListener ;
  private LinkedList<NodeWatcher> watcherQueue = new LinkedList<>();
  
  public WaitingNodeEventListener(Registry registry) {
    registryListener = new RegistryListener(registry);
  }
  
  public int getWaitingNodeEventCount() { return watcherQueue.size(); }
  
  public void add(String path, NodeEvent.Type ... type) throws Exception {
    registryListener.watch(path, new NodeEventTypeNodeWatcher(path, type));
  }
  
  public <T> void add(String path, T data) throws Exception {
    registryListener.watch(path, new DataChangeNodeWatcher<T>((Class<T>)data.getClass(), data));
  }
  
  synchronized public void waitForEvents(long timeout) throws Exception {
    if(watcherQueue.size() == 0) return ;
    long stopTime = System.currentTimeMillis() + timeout;
    try {
      while(true) {
        long waitTime = stopTime - System.currentTimeMillis();
        if(waitTime <= 0) return;
        wait(waitTime);
        if(watcherQueue.size() == 0) return ;
      }
    } catch (InterruptedException e) {
      throw new Exception("Cannot wait for the events in " + timeout + "ms") ;
    } finally {
      if(watcherQueue.size() > 0) {
      }
    }
  }
  
  synchronized void onDetectNodeEvent(NodeWatcher watcher, NodeEvent event) {
    NodeWatcher waitingWatcher = watcherQueue.getFirst() ;
    if(waitingWatcher == watcher) {
      watcherQueue.removeFirst();
    }
  }
  
  class NodeEventTypeNodeWatcher extends NodeWatcher {
    private String   path ;
    NodeEvent.Type[] type;
    
    NodeEventTypeNodeWatcher(String path, NodeEvent.Type[] type) {
      this.type = type;
    }
    
    @Override
    public void onEvent(NodeEvent event) throws Exception {
      for(int i = 0; i < type.length; i++) {
        if(type[i] == event.getType()) {
          //match the event type
          onDetectNodeEvent(this, event);
        }
      }
    }

    public String toString() {
      StringBuilder b = new StringBuilder() ; 
      b.append("Waiting for the event = [");
      for(int i = 0; i < type.length; i++) {
        if(i > 0) b.append(",");
        b.append(type[i]);
      }
      b.append("], path = " + path);
      return b.toString();
    }
  }
  
  class DataChangeNodeWatcher<T> extends NodeWatcher {
    private String   path ;
    private Class<T> dataType ;
    private T        expectData ;
    
    DataChangeNodeWatcher(Class<T> dataType, T expectData) {
      this.dataType = dataType;
      this.expectData = expectData ;
    }
    
    @Override
    public void onEvent(NodeEvent event) throws Exception {
      T data = registryListener.getRegistry().getDataAs(event.getPath(), dataType) ;
      if(expectData.equals(data)) {
        onDetectNodeEvent(this, event);
      }
    }
    
    public String toString() {
      StringBuilder b = new StringBuilder() ; 
      b.append("Waiting for the data ");
      b.append("], path = " + path);
      return b.toString();
    }
  }
}
