package com.neverwinterdp.scribengin.dataflow.util;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.RefNode;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.util.NodeFormatter;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowTaskExecutorDescriptor;
import com.neverwinterdp.util.text.TabularFormater;

public class DataflowVMDetailedFormatter extends NodeFormatter {
  private Node node;
  
  public DataflowVMDetailedFormatter(Node n){
    this.node = n;
  }
  
 // /scribengin/dataflows/running/kafka-to-kafka/workers/active/kafka-to-kafka-worker-1
 // /scribengin/dataflows/running/kafka-to-kafka/master/leader
/*
dataflow
  workers
      history
      active
        kafka-to-kafka-worker-1 - {"path":"/vm/allocated/kafka-to-kafka-worker-1"}
          executors
            executor-0 - {"id":"executor-0","status":"RUNNING","assignedTaskIds":[0,3,5,7,9,0,4,6,7,9]
        kafka-to-kafka-worker-2 - {"path":"/vm/allocated/kafka-to-kafka-worker-2"}
          executors
            executor-0 - {"id":"executor-0","status":"TERMINATED","assignedTaskIds":[1]}
        kafka-to-kafka-worker-3 - {"path":"/vm/allocated/kafka-to-kafka-worker-3"}
          executors
            executor-0 - {"id":"executor-0","status":"TERMINATED","assignedTaskIds":[2,4,6,8,1,2,3,5,8
  status - "FINISH"
  master
    leader - {"path":"/vm/allocated/kafka-to-kafka-master-1"}
 */
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
      
      TabularFormater workerNodeTable = new TabularFormater("Name", "Path");
      workerNodeTable.setTitle("DataflowVM");
      workerNodeTable.setIndent("  ");
      
      TabularFormater executorNodeTable = new TabularFormater("Worker", "Executor ID", "Status","AssignedTaskIds");
      executorNodeTable.setTitle("Executors");
      executorNodeTable.setIndent("  ");
      
      
      Node master = dataflowNode.getChild("master").getChild("leader");
      workerNodeTable.addRow("DataflowMaster", master.getDataAs(Map.class).get("path"));
      
      List<String> workerIds = dataflowNode.getDescendant("workers/active").getChildren();
      for(String workerId : workerIds){
        Node worker = dataflowNode.getDescendant("workers/active/" + workerId);
        RefNode refVMDescriptor = worker.getDataAs(RefNode.class) ;
        if(refVMDescriptor == null) {
          System.err.println("Cannot get the data for " + worker.getPath());
          try {
            dataflowNode.getDescendant("workers/active").dump(System.err);
          } catch (IOException e) {
            e.printStackTrace();
          }
          continue;
        }
        workerNodeTable.addRow(worker.getName(), refVMDescriptor.getPath());
        if(worker.hasChild("executors")){
          for(String executorNode: worker.getChild("executors").getChildren()){
            Node executor = worker.getChild("executors").getChild(executorNode);
            DataflowTaskExecutorDescriptor desc = executor.getDataAs(DataflowTaskExecutorDescriptor.class);
            String taskIds = StringUtils.join(desc.getAssignedTaskIds(), ",");
            executorNodeTable.addRow(worker.getName(), desc.getId(),desc.getStatus(), taskIds);
          }
        }
      }
      
      
      
      b.append(workerNodeTable.getFormatText());
      b.append("\n");
      b.append(executorNodeTable.getFormatText());
      b.append("\n");
    } catch (RegistryException e) {
      e.printStackTrace();
      b.append(e.getMessage());
    }
    return b.toString();
  }

}
