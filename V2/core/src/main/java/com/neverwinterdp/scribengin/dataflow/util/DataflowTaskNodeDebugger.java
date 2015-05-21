package com.neverwinterdp.scribengin.dataflow.util;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.task.TaskTransactionId;
import com.neverwinterdp.registry.util.NodeDebugger;
import com.neverwinterdp.registry.util.NodeFormatter;
import com.neverwinterdp.registry.util.RegistryDebugger;

public class DataflowTaskNodeDebugger implements NodeDebugger{
  boolean detailedDebugger;

  public DataflowTaskNodeDebugger(boolean detailedDebugger){
    this.detailedDebugger = detailedDebugger;
  }

  public DataflowTaskNodeDebugger(){
    this(false);
  }
  
  @Override
  public void onCreate(RegistryDebugger registryDebugger, Node assignedTaskNode) throws Exception {
    registryDebugger.println("DataflowTaskNodeDebugger: Node = " + assignedTaskNode.getPath() + ", Event = CREATE");
    TaskTransactionId taskTransactionId = new TaskTransactionId(assignedTaskNode.getName()) ;
    Node tasksNode = assignedTaskNode.getParentNode().getParentNode().getParentNode().getParentNode();
    Node taskDescriptorNode = tasksNode.getDescendant("task-list/" + taskTransactionId.getTaskId()); 
    
    NodeFormatter formatter = null;
    if(this.detailedDebugger){
      formatter = new DataflowTaskRegistryDetailedFormater(taskDescriptorNode);
    }
    else{
      formatter = new DataflowTaskRegistrySimpleFormater(taskDescriptorNode);
    }
    registryDebugger.println(formatter.getFormattedText());
  }

  @Override
  public void onModify(RegistryDebugger registryDebugger, Node node) throws Exception {
  }

  @Override
  public void onDelete(RegistryDebugger registryDebugger, Node node) throws Exception {
  }
}
