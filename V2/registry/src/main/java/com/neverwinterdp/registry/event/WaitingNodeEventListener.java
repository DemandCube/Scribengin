package com.neverwinterdp.registry.event;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.util.text.TabularFormater;

abstract public class WaitingNodeEventListener {
  protected RegistryListener        registryListener;
  protected LinkedList<WaitingNodeEventWatcher> watcherQueue  = new LinkedList<>();
  protected int                     waitingNodeEventCount = 0;
  protected int                     detectNodeEventCount  = 0;
  protected List<NodeEventLog>      eventLogs = new ArrayList<NodeEventLog>() ; 
  protected long                    estimateLastDetectEventTime = 0 ;
  
  public WaitingNodeEventListener(Registry registry) {
    registryListener = new RegistryListener(registry);
  }
  
  public int getWaitingNodeEventCount() { return waitingNodeEventCount; }
  
  public int getDetectNodeEventCount() { return detectNodeEventCount ; }
  
  public int getUndetectNodeEventCount() { return watcherQueue.size() ; }
  
  public void add(String path, NodeEvent.Type type) throws Exception {
    add(path, type, null);
  }
  
  public void add(String path, NodeEvent.Type type, String desc) throws Exception {
    add(path, new NodeEvent.Type[] { type }, desc);
  }
  
  synchronized public void add(String path, NodeEvent.Type[] type, String desc) throws Exception {
    WaitingNodeEventWatcher watcher = new NodeEventTypeNodeWatcher(path, type, desc);
    watcherQueue.addLast(watcher);
    registryListener.watch(path, watcher, false);
    waitingNodeEventCount++;
    if(detectNodeEventCount == 0) estimateLastDetectEventTime = System.currentTimeMillis();
  }
  
  public <T> void add(String path, T expectData) throws Exception {
    add(path, expectData, null);
  }
  
  /**
   * Add a data change node watcher to detect when the data in the node match with the expect data
   * @param path
   * @param expectData
   * @throws Exception
   */
  synchronized public <T> void add(String path, T expectData, String desc) throws Exception {
    WaitingNodeEventWatcher watcher = new DataChangeNodeWatcher<T>(path, (Class<T>)expectData.getClass(), expectData, desc);
    watcherQueue.addLast(watcher);
    registryListener.watch(path, watcher, true);
    waitingNodeEventCount++;
    if(detectNodeEventCount == 0) estimateLastDetectEventTime = System.currentTimeMillis() ;
  }
  
  public <T> void add(String path, NodeEventMatcher matcher) throws Exception {
    add(path, matcher, null);
  }
  
  synchronized public <T> void add(String path, NodeEventMatcher matcher,String desc) throws Exception {
    WaitingNodeEventWatcher watcher = new NodeEventMatcherWatcher(path, matcher, desc);
    watcherQueue.addLast(watcher);
    registryListener.watch(path, watcher, true);
    waitingNodeEventCount++;
    if(detectNodeEventCount == 0) estimateLastDetectEventTime = System.currentTimeMillis() ;
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
  
  public TabularFormater getTabularFormaterEventLogInfo() {
    String[] header = {"Category", "Event", "Path", "Wait Time", "Watcher"} ;
    TabularFormater infoFormater = new TabularFormater(header);
    if(eventLogs.size() > 0) {
      infoFormater.addRow("Triggered", "", "", "", "");
      for(NodeEventLog sel : eventLogs) {
        infoFormater.addRow("", sel.getEvent(), sel.getPath(), sel.getWaitTime(), sel.getWatcherDescription());
      }
    } ;
    if(watcherQueue.size() > 0) {
      infoFormater.addRow("Waiting", "", "", "", "");
      for(WaitingNodeEventWatcher sel : watcherQueue) {
        infoFormater.addRow("", "", sel.getPath(), "", sel.toString());
      }
    }
    return infoFormater ;
  }
  
  abstract protected void onDetectNodeEvent(NodeWatcher watcher, NodeEvent event) ;
  
  abstract static public class WaitingNodeEventWatcher extends NodeWatcher {
    String path ;
    String description ;
    
    public String getPath() { return path ; }
    
    public String getDescription() { return description ; }
    
  }
  
  class NodeEventTypeNodeWatcher extends WaitingNodeEventWatcher {
    NodeEvent.Type[] type;
    
    NodeEventTypeNodeWatcher(String path, NodeEvent.Type[] type, String desc) {
      this.path = path;
      this.type = type;
      this.description = desc;
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
      if(description != null) return description; 
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
  
  class DataChangeNodeWatcher<T> extends WaitingNodeEventWatcher {
    private Class<T> dataType ;
    private T        expectData ;
    
    DataChangeNodeWatcher(String path, Class<T> dataType, T expectData, String desc) {
      this.path = path ;
      this.dataType = dataType;
      this.expectData = expectData ;
      this.description = desc ;
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
      if(description != null) return description ;
      StringBuilder b = new StringBuilder() ; 
      b.append("Waiting for the data on path = " + path + " data = " + JSONSerializer.INSTANCE.toString(expectData));
      return b.toString();
    }
  }
  
  public class NodeEventMatcherWatcher extends WaitingNodeEventWatcher {
    private NodeEventMatcher matcher;
    
    public NodeEventMatcherWatcher(String path, NodeEventMatcher matcher) {
      this.path    = path;
      this.matcher = matcher;
    }
    
    public NodeEventMatcherWatcher(String path, NodeEventMatcher matcher, String desc) {
      this(path, matcher) ;
      description = desc;
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
      if(description != null) return description;
      StringBuilder b = new StringBuilder() ; 
      b.append("Waiting for the node event matcher on path = " + path);
      return b.toString();
    }
  }
  
  static public class NodeEventLog {
    private long   waitTime ;
    private String event ;
    private String path ;
    private String watcherDescription ;
    
    public NodeEventLog(long waitTime, NodeEvent event, NodeWatcher watcher) {
      this.waitTime = waitTime ;
      this.event = event.getType().toString();
      this.path  = event.getPath() ;
      this.watcherDescription = watcher.toString() ;
    }

    public long getWaitTime() { return waitTime; }

    public String getEvent() { return event; }

    public String getPath() { return path; }

    public String getWatcherDescription() { return watcherDescription; }
  }
}