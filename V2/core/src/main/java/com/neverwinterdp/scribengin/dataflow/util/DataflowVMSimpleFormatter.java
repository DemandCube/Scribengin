package com.neverwinterdp.scribengin.dataflow.util;

import java.util.List;
import java.util.Map;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.util.NodeFormatter;

public class DataflowVMSimpleFormatter extends NodeFormatter{
  private Node node;
  
  public DataflowVMSimpleFormatter(Node n){
    this.node = n;
  }
  
  @Override
  public String getFormattedText() {
    StringBuilder b = new StringBuilder();
    try {
      Node dataflowNode = null;
      if(this.node.getName().equalsIgnoreCase("leader")){
        dataflowNode = this.node.getParentNode().getParentNode();
      } else if(this.node.getParentNode().getName().equalsIgnoreCase("executors")){
        dataflowNode = this.node.getParentNode().getParentNode().getParentNode().getParentNode().getParentNode();
      } else{
        dataflowNode = this.node.getParentNode().getParentNode().getParentNode();
      }
      
      Node master = dataflowNode.getChild("master").getChild("leader");
      b.append("  DataflowMaster: "+master.getDataAs(Map.class).get("path")+"\n");
      b.append("  Workers: ");
      List<String> workerIds = dataflowNode.getDescendant("workers/active").getChildren();
      for(String workerId : workerIds){
        Node worker = dataflowNode.getDescendant("workers/active/" + workerId);
        b.append(worker.getName() + ": " + worker.getDataAs(Map.class).get("path") + ", ");
      }
      
    } catch (Exception e) {
      e.printStackTrace();
      b.append(e.getMessage());
    }
    return b.toString().substring(0, b.toString().length()-2)+"\n";
  }

}
