package com.neverwinterdp.registry.util;

import java.util.LinkedList;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeWatcher;
import com.neverwinterdp.registry.event.RegistryListener;
import com.neverwinterdp.util.JSONSerializer;

public class WaitingNodeEventListener {
  private RegistryListener registryListener ;
  private LinkedList<NodeWatcher> watcherQueue = new LinkedList<>();
  private int waitingNodeEventCount = 0;
  private int detectNodeEventCount = 0 ;
  
  public WaitingNodeEventListener(Registry registry) {
    registryListener = new RegistryListener(registry);
  }
  
  public int getWaitingNodeEventCount() { return waitingNodeEventCount; }
  
  public int getDetectNodeEventCount() { return detectNodeEventCount ; }
  
  synchronized public void add(String path, NodeEvent.Type type) throws Exception {
    NodeWatcher watcher = new NodeEventTypeNodeWatcher(path, new NodeEvent.Type[] { type });
    watcherQueue.addLast(watcher);
    registryListener.watch(path, watcher);
    waitingNodeEventCount++;
  }
  
  synchronized public void add(String path, NodeEvent.Type ... type) throws Exception {
    NodeWatcher watcher = new NodeEventTypeNodeWatcher(path, type);
    watcherQueue.addLast(watcher);
    registryListener.watch(path, watcher, false);
    waitingNodeEventCount++;
  }
  
  synchronized public <T> void add(String path, T data) throws Exception {
    NodeWatcher watcher = new DataChangeNodeWatcher<T>(path, (Class<T>)data.getClass(), data);
    watcherQueue.addLast(watcher);
    registryListener.watch(path, watcher, true);
    waitingNodeEventCount++;
  }
  
  synchronized public void waitForEvents(long timeout) throws Exception {
    if(detectNodeEventCount == waitingNodeEventCount) return ;
    long stopTime = System.currentTimeMillis() + timeout;
    try {
      while(detectNodeEventCount < waitingNodeEventCount) {
        long waitTime = stopTime - System.currentTimeMillis();
        if(waitTime <= 0) return;
        wait(waitTime);
      }
    } catch (InterruptedException e) {
      throw new Exception("Cannot wait for the events in " + timeout + "ms") ;
    }
  }
  
  synchronized void onDetectNodeEvent(NodeWatcher watcher, NodeEvent event) {
    NodeWatcher waitingWatcher = watcherQueue.getFirst() ;
    if(waitingWatcher == watcher) {
      watcherQueue.removeFirst();
      detectNodeEventCount++;
      notifyAll();
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
    
    DataChangeNodeWatcher(String path, Class<T> dataType, T expectData) {
      this.path = path ;
      this.dataType = dataType;
      this.expectData = expectData ;
    }
    
    @Override
    public void onEvent(NodeEvent event) throws Exception {
      T data = registryListener.getRegistry().getDataAs(event.getPath(), dataType) ;
      System.out.println("Got data change event: " + JSONSerializer.INSTANCE.toString(data));
      if(expectData.equals(data)) {
        onDetectNodeEvent(this, event);
        setComplete();
      }
    }
    
    public String toString() {
      StringBuilder b = new StringBuilder() ; 
      b.append("Waiting for the data on path = " + path);
      return b.toString();
    }
  }
}
