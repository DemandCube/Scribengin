package com.neverwinterdp.scribengin.dataflow.util;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.util.NodeDebugger;
import com.neverwinterdp.registry.util.RegistryDebugger;

public class DataflowTaskNodeDebugger implements NodeDebugger{
  @Override
  public void onCreate(RegistryDebugger registryDebugger, Node node) throws Exception {
    
    DataflowTaskRegistryFormatter formatter = new DataflowTaskRegistryFormatter(node);
    registryDebugger.println("RegistryDebugger: Node = " + node.getPath() + ", Event = CREATE");
    registryDebugger.println(formatter.getFormattedText());

    registryDebugger.watchModify(node.getPath(), formatter, true);
    registryDebugger.watch(node.getPath() + "/report", formatter, true);
  }

  @Override
  public void onModify(RegistryDebugger registryDebugger, Node node) throws Exception {
  }

  @Override
  public void onDelete(RegistryDebugger registryDebugger, Node node) throws Exception {
  }
}
