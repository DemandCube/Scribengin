package com.neverwinterdp.registry.util;

import java.io.IOException;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeWatcher;
import com.neverwinterdp.registry.event.RegistryListener;

public class RegistryDebugger {
  private RegistryListener registryListener ;
  private Appendable out ;
  
  public RegistryDebugger(Appendable out, Registry registry) {
    registryListener = new RegistryListener(registry);
    this.out = out ;
  }
  
  public Registry getRegistry(){
    return registryListener.getRegistry();
  }
  
  public void clear() {
    registryListener.clear();
  }
  
  public void watch(String path, NodeFormatter formater, boolean persistent) throws RegistryException {
    registryListener.watch(path, new NodeFormatterWatcher(formater), persistent);
  }
  
  public void watchModify(String path, NodeFormatter formater, boolean persistent) throws RegistryException {
    registryListener.watchModify(path, new NodeFormatterWatcher(formater), persistent);
  }
  
  public void watchChildren(String path, NodeFormatter formater, boolean persistent) throws RegistryException {
    registryListener.watchChildren(path, new NodeFormatterWatcher(formater), persistent);
  }
  
  public void watchChildren(String path, NodeFormatter formater, boolean persistent, boolean waitIfNotExist) throws RegistryException {
    registryListener.watchChildren(path, new NodeFormatterWatcher(formater), persistent, true);
  }
  
  public void watch(String path, NodeDebugger nodeDebugger, boolean persistent) throws RegistryException {
    registryListener.watch(path, new NodeDebuggerWatcher(nodeDebugger), persistent);
  }
  
  public void watchChild(String path, String childExp, NodeDebugger nodeDebugger) throws RegistryException {
    registryListener.watchChild(path, childExp, new NodeDebuggerWatcher(nodeDebugger));
  }
  
  public void watchChild(String path, String childExp, NodeFormatter formater) throws RegistryException {
    registryListener.watchChild(path, childExp, new NodeFormatterWatcher(formater));
  }
  
  public void println(String text) throws IOException {
    out.append(text).append("\n") ;
  }
  
  public class NodeFormatterWatcher extends NodeWatcher {
    private NodeFormatter formater ;
    
    public NodeFormatterWatcher(NodeFormatter formater) {
      this.formater = formater ;
    }
    
    @Override
    public void onEvent(NodeEvent event) throws Exception {
      Node node = registryListener.getRegistry().get(event.getPath());
      String text = formater.getFormattedText();
      println("RegistryDebugger: Node = " + node.getPath() + ", event = " + event.getType()) ;
      println(text) ;
    }
  }
  
  public class NodeDebuggerWatcher extends NodeWatcher {
    private NodeDebugger nodeDebugger ;
    
    public NodeDebuggerWatcher(NodeDebugger nodeDebugger) {
      this.nodeDebugger = nodeDebugger;
    }
    
    @Override
    public void onEvent(NodeEvent event) throws Exception {
      Node node = registryListener.getRegistry().get(event.getPath());
      if(event.getType() == NodeEvent.Type.CREATE) {
        nodeDebugger.onCreate(RegistryDebugger.this, node);
      } else if(event.getType() == NodeEvent.Type.MODIFY) {
        nodeDebugger.onModify(RegistryDebugger.this, node);
      } else if(event.getType() == NodeEvent.Type.DELETE) {
        nodeDebugger.onDelete(RegistryDebugger.this, node);
      } else {
        throw new Exception("Unknown event type " + event.getType() + " for the path " + event.getPath()) ;
      }
    }
  }
}
