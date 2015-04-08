package com.neverwinterdp.scribengin.dataflow.util;

import java.util.Map;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.util.NodeDebugger;
import com.neverwinterdp.registry.util.RegistryDebugger;
import com.neverwinterdp.vm.util.VMRegistryFormatter;

public class DataflowWorkerNodeDebugger implements NodeDebugger{
  
  @Override
  public void onCreate(RegistryDebugger registryDebugger, Node node) throws Exception {
    //Grabs the path from the node passed in
    //Adds a watch for the node in that path using VMNodeDebugger
    registryDebugger.println("RegistryDebugger: Node = " + node.getPath() + ", Event = CREATE");
    Map<?,?> nodeData = node.getDataAs(Map.class);
    
    Node vmNode = new Node(registryDebugger.getRegistry(), (String)nodeData.get("path"));
    VMRegistryFormatter vmformatter = new VMRegistryFormatter(vmNode);
    
    registryDebugger.watch((String)nodeData.get("path"), vmformatter, true);
    
  }

  @Override
  public void onModify(RegistryDebugger registryDebugger, Node node) throws Exception {
  }

  @Override
  public void onDelete(RegistryDebugger registryDebugger, Node node) throws Exception {
  }
}
