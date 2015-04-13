package com.neverwinterdp.scribengin.dataflow.util;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.util.NodeDebugger;
import com.neverwinterdp.registry.util.RegistryDebugger;

//TODO: Implement DataflowTaskNodeDebugger and DataflowTaskNodeDetailDebugger which print out the data 
//in simple(in 1 or 2 lines) format or in detail format
//recustomize the formater to allow indent setting. The print out should have the format:
//RegistryDebugger: Node = /scribengin/dataflows/running/kafka-to-kafka/tasks/executors/assigned/task-0000000000, Event = CREATE
//  (indent) table of info or other info 
//Add:
//
// --debug-dataflow-task and --debug-dataflow-task-detail
// --debug-dataflow-worker and --debug-dataflow-task-detail
public class DataflowTaskNodeDebugger implements NodeDebugger{
  
  @Override
  public void onCreate(RegistryDebugger registryDebugger, Node assignedTaskNode) throws Exception {
    registryDebugger.println("DataflowTaskNodeDebugger: Node = " + assignedTaskNode.getPath() + ", Event = CREATE");
    String assignedTaskName = assignedTaskNode.getName();
    Node tasksNode = assignedTaskNode.getParentNode().getParentNode().getParentNode();
    Node tasksDescriptorsNode = tasksNode.getChild("descriptors");
    Node taskDescriptorNode = tasksDescriptorsNode.getChild(assignedTaskName); 
    
    DataflowTaskRegistryDetailFormater formatter = new DataflowTaskRegistryDetailFormater(taskDescriptorNode);
    registryDebugger.println(formatter.getFormattedText());
    registryDebugger.watchModify(assignedTaskNode.getChild("heartbeat").getPath(), formatter, true);
  }

  @Override
  public void onModify(RegistryDebugger registryDebugger, Node node) throws Exception {
  }

  @Override
  public void onDelete(RegistryDebugger registryDebugger, Node node) throws Exception {
  }
}
