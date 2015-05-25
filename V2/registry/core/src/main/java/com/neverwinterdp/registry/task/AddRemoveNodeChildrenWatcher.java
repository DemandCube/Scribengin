package com.neverwinterdp.registry.task;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.event.NodeChildrenWatcher;
import com.neverwinterdp.registry.event.NodeEvent;

public class AddRemoveNodeChildrenWatcher<T> extends NodeChildrenWatcher {
  private TreeSet<String> trackingChildrenSet ;
  
  public AddRemoveNodeChildrenWatcher(Registry registry, String path) throws RegistryException {
    super(registry, true);
    trackingChildrenSet = new TreeSet<String>(registry.getChildren(path)) ;
    watchChildren(path);
  }
  
  public AddRemoveNodeChildrenWatcher(Registry registry, Node node) throws RegistryException {
    this(registry, node.getPath());
  }

  @Override
  synchronized public void processNodeEvent(NodeEvent event) throws Exception {
    if(event.getType() == NodeEvent.Type.CHILDREN_CHANGED) {
      List<String> childrenList = getRegistry().getChildren(event.getPath());
      for(String selTask : childrenList) {
        if(!trackingChildrenSet.contains(selTask)) {
          trackingChildrenSet.add(selTask);
          onAddChild(selTask);
        }
      }
      Set<String> childrenSet = new HashSet<String>(childrenList) ;
      Iterator<String> i = trackingChildrenSet.iterator();
      while(i.hasNext()) {
        String selTask = i.next();
        if(!childrenSet.contains(selTask)) {
          i.remove();
          onRemoveChild(selTask);
        }
      }
    }
  }
  
  public void onRemoveChild(String taskId) { } 
  
  public void onAddChild(String taskId) { } 
}