package com.neverwinterdp.vm.util;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.util.NodeDebugger;
import com.neverwinterdp.registry.util.NodeFormater;
import com.neverwinterdp.registry.util.RegistryDebugger;

//TODO: make the output prettier with the VMDescriptor information
public class VMNodeDebugger implements NodeDebugger {
  @Override
  public void onCreate(RegistryDebugger registryDebugger, Node node) throws Exception {
    NodeFormater formater = new NodeFormater.NodeDumpFormater(node, "  ");

    registryDebugger.println("RegistryDebugger: Node = " + node.getPath() + ", Event = CREATE");
    registryDebugger.println(formater.getFormattedText());

    registryDebugger.watchModify(node.getPath(), formater, true);
    registryDebugger.watch(node.getPath() + "/status", formater, true);
  }

  @Override
  public void onModify(RegistryDebugger registryDebugger, Node node) throws Exception {
  }

  @Override
  public void onDelete(RegistryDebugger registryDebugger, Node node) throws Exception {
  }
}
