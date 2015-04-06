package com.neverwinterdp.registry.util;

import java.io.IOException;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeWatcher;
import com.neverwinterdp.registry.event.RegistryListener;

public class RegistryDebugger extends RegistryListener {
  private Appendable out ;
  
  public RegistryDebugger(Appendable out, Registry registry) {
    super(registry);
    this.out = out ;
  }
  
  public void watch(String path, NodeFormater formater, boolean persistent) throws RegistryException {
    watch(path, new NodeFormatterWatcher(formater), persistent);
  }
  
  public void watchModify(String path, NodeFormater formater, boolean persistent) throws RegistryException {
    watchModify(path, new NodeFormatterWatcher(formater), persistent);
  }
  
  public void watchChildren(String path, NodeFormater formater, boolean persistent) throws RegistryException {
    watchChildren(path, new NodeFormatterWatcher(formater), persistent);
  }
  
  public void watchChildren(String path, NodeFormater formater, boolean persistent, boolean waitIfNotExist) throws RegistryException {
    watchChildren(path, new NodeFormatterWatcher(formater), persistent, true);
  }
  
  public void watch(String path, NodeDebugger nodeDebugger, boolean persistent) throws RegistryException {
    watch(path, new NodeDebuggerWatcher(nodeDebugger), persistent);
  }
  
  public void println(String text) throws IOException {
    out.append(text).append("\n") ;
  }
  
  public class NodeFormatterWatcher extends NodeWatcher {
    private NodeFormater formater ;
    
    public NodeFormatterWatcher(NodeFormater formater) {
      this.formater = formater ;
    }
    
    @Override
    public void onEvent(NodeEvent event) {
      try {
        Node node = getRegistry().get(event.getPath());
        String text = formater.getFormattedText();
        println("RegistryDebugger: Node = " + node.getPath() + ", event = " + event.getType()) ;
        println(text) ;
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
  
  public class NodeDebuggerWatcher extends NodeWatcher {
    private NodeDebugger nodeDebugger ;
    
    public NodeDebuggerWatcher(NodeDebugger nodeDebugger) {
      this.nodeDebugger = nodeDebugger;
    }
    
    @Override
    public void onEvent(NodeEvent event) throws Exception {
      Node node = getRegistry().get(event.getPath());
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
