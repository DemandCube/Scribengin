package com.neverwinterdp.registry.event;

import java.util.LinkedList;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.text.TabularFormater;

public class WaitingNodeEventListener {
  private RegistryListener        registryListener;
  private LinkedList<NodeWatcher> watcherQueue          = new LinkedList<>();
  private int                     waitingNodeEventCount = 0;
  private int                     detectNodeEventCount  = 0;
  private NodeEvent               lastDetectNodeEvent;
  private NodeWatcher             lastHandledNodeWatcher;

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
  
  /**
   * Add a data change node watcher to detect when the data in the node match with the expect data
   * @param path
   * @param expectData
   * @throws Exception
   */
  synchronized public <T> void add(String path, T expectData) throws Exception {
    NodeWatcher watcher = new DataChangeNodeWatcher<T>(path, (Class<T>)expectData.getClass(), expectData);
    watcherQueue.addLast(watcher);
    registryListener.watch(path, watcher, true);
    waitingNodeEventCount++;
  }
  
  synchronized public <T> void add(String path, NodeEventMatcher matcher) throws Exception {
    NodeWatcher watcher = new NodeEventMatcherWatcher(path, matcher);
    watcherQueue.addLast(watcher);
    registryListener.watch(path, watcher, true);
    waitingNodeEventCount++;
  }
  
  synchronized public void waitForEvents(long timeout) throws Exception {
    if(detectNodeEventCount == waitingNodeEventCount) return ;
    long stopTime = System.currentTimeMillis() + timeout;
    try {
      while(detectNodeEventCount < waitingNodeEventCount) {
        long maxWaitTime = stopTime - System.currentTimeMillis();
        if(maxWaitTime <= 0) return;
        wait(maxWaitTime);
      }
    } catch (InterruptedException e) {
      throw new Exception("Cannot wait for the events in " + timeout + "ms") ;
    } 
  }
  
  synchronized public TabularFormater waitForEventsWithInfo(long timeout) throws Exception {
    String[] header = {"Event", "Path", "Wait Time", "Watcher"} ;
    TabularFormater infoFormater = new TabularFormater(header);
    if(detectNodeEventCount == waitingNodeEventCount) return infoFormater ;
    long stopTime = System.currentTimeMillis() + timeout;
    try {
      while(detectNodeEventCount < waitingNodeEventCount) {
        long maxWaitTime = stopTime - System.currentTimeMillis();
        if(maxWaitTime <= 0) return infoFormater ;
        long startWait = System.currentTimeMillis() ;
        wait(maxWaitTime);
        long waitTime = System.currentTimeMillis() - startWait;
        NodeWatcher watcher = lastHandledNodeWatcher ;
        if(watcher == null) watcher = watcherQueue.getFirst();
        Object[] cell = {
          lastDetectNodeEvent != null ? lastDetectNodeEvent.getType() : "unknown",
          lastDetectNodeEvent != null ? lastDetectNodeEvent.getPath() : "unknown",
          waitTime,
          watcher.toString()
        };
        infoFormater.addRow(cell);
      }
    } catch (InterruptedException e) {
      StringBuilder b = new StringBuilder() ;
      b.append("Cannot wait for the events in " + timeout + "ms\n") ;
      b.append(infoFormater.getFormatText());
      throw new Exception(b.toString()) ;
    } 
    return infoFormater;
  }
  
  synchronized void onDetectNodeEvent(NodeWatcher watcher, NodeEvent event) {
    NodeWatcher waitingWatcher = watcherQueue.getFirst() ;
    if(waitingWatcher == watcher) {
      watcherQueue.removeFirst();
      detectNodeEventCount++;
      lastDetectNodeEvent = event;
      lastHandledNodeWatcher = watcher;
      notifyAll();
    }
  }
  
  class NodeEventTypeNodeWatcher extends NodeWatcher {
    private String   path ;
    NodeEvent.Type[] type;
    
    NodeEventTypeNodeWatcher(String path, NodeEvent.Type[] type) {
      this.path = path;
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
      if(event.getType() == NodeEvent.Type.CREATE || event.getType() == NodeEvent.Type.MODIFY) {
        T data = registryListener.getRegistry().getDataAs(event.getPath(), dataType) ;
        if(expectData.equals(data)) {
          onDetectNodeEvent(this, event);
          setComplete();
        }
      } else if(event.getType() == NodeEvent.Type.DELETE) {
        setComplete();
      }
    }
    
    public String toString() {
      StringBuilder b = new StringBuilder() ; 
      b.append("Waiting for the data on path = " + path + " data = " + JSONSerializer.INSTANCE.toString(expectData));
      return b.toString();
    }
  }
  
  public class NodeEventMatcherWatcher extends NodeWatcher {
    private String path  ;
    private NodeEventMatcher matcher;
    
    public NodeEventMatcherWatcher(String path, NodeEventMatcher matcher) {
      this.path    = path;
      this.matcher = matcher;
    }
    
    @Override
    public void onEvent(NodeEvent event) throws Exception {
      Node node = registryListener.getRegistry().get(event.getPath()) ;
      if(matcher.matches(node, event)) {
        onDetectNodeEvent(this, event);
        setComplete();
      }
    }
    
    public String toString() {
      StringBuilder b = new StringBuilder() ; 
      b.append("Waiting for the node event matcher on path = " + path);
      return b.toString();
    }
  }
}
