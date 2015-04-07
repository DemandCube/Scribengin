package com.neverwinterdp.scribengin.dataflow.util;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.util.NodeDebugger;
import com.neverwinterdp.registry.util.RegistryDebugger;

public class DataflowWorkerNodeDebugger implements NodeDebugger{
  @Override
  public void onCreate(RegistryDebugger registryDebugger, Node node) throws Exception {
    
    DataflowWorkerRegistryFormatter formatter = new DataflowWorkerRegistryFormatter(node);
    registryDebugger.println("RegistryDebugger: Node = " + node.getPath() + ", Event = CREATE");
    registryDebugger.println(formatter.getFormattedText());

    registryDebugger.watchModify(node.getPath(), formatter, true);
    registryDebugger.watch(node.getPath() + "/executors", formatter, true);
  }

  @Override
  public void onModify(RegistryDebugger registryDebugger, Node node) throws Exception {
  }

  @Override
  public void onDelete(RegistryDebugger registryDebugger, Node node) throws Exception {
  }
}
