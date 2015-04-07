package com.neverwinterdp.vm.util;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.util.NodeDebugger;
import com.neverwinterdp.registry.util.RegistryDebugger;


public class VMNodeDebugger implements NodeDebugger {
  @Override
  public void onCreate(RegistryDebugger registryDebugger, Node node) throws Exception {
    VMRegistryFormatter formatter = new VMRegistryFormatter(node);
    registryDebugger.println("RegistryDebugger: Node = " + node.getPath() + ", Event = CREATE");
    registryDebugger.println(formatter.getFormattedText());

    registryDebugger.watchModify(node.getPath(), formatter, true);
    registryDebugger.watch(node.getPath() + "/status", formatter, true);
  }

  @Override
  public void onModify(RegistryDebugger registryDebugger, Node node) throws Exception {
  }

  @Override
  public void onDelete(RegistryDebugger registryDebugger, Node node) throws Exception {
  }
}
