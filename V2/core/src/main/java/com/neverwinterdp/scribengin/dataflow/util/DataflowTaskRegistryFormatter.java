package com.neverwinterdp.scribengin.dataflow.util;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.util.NodeFormatter;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskReport;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.VMDescriptor;


public class DataflowTaskRegistryFormatter extends NodeFormatter {
  private Node taskNode ;
  
  public DataflowTaskRegistryFormatter(Node vmNode) {
    this.taskNode = vmNode;
  }
  
  @Override
  public String getFormattedText() {
    StringBuilder b = new StringBuilder() ;
    try {
      if(!taskNode.exists()){
        return "Dataflow task node is already deleted or moved to the history";
      }
      
      //If we're watching path [dataflowname]/tasks/executors/available
      //We need to then grab the heartbeat node to get the descriptor there
      if(!taskNode.getName().equalsIgnoreCase("heartbeat")){
        taskNode = taskNode.getChild("heartbeat");
      }
      
      VMDescriptor heartbeatDesc = taskNode.getDataAs(VMDescriptor.class);
      String taskName = taskNode.getParentNode().getName();
      //Move up the registry tree and get the taskDescriptor and report
      Node taskDescNode = taskNode.getParentNode().getParentNode().getParentNode().
                                  getParentNode().getChild("descriptors").getChild(taskName);
      DataflowTaskDescriptor dataflowDescriptor = taskDescNode.getDataAs(DataflowTaskDescriptor.class);
      DataflowTaskReport     taskReport         = taskDescNode.getChild("report").getDataAs(DataflowTaskReport.class);
      
      TabularFormater formatter = new TabularFormater("DatflowTaskKey", "Value");
      
      formatter.addRow("ID",heartbeatDesc.getId());
      formatter.addRow("Stored Path", heartbeatDesc.getStoredPath());
      formatter.addRow("Hostname",heartbeatDesc.getHostname());
      formatter.addRow("Memory",heartbeatDesc.getMemory());
      formatter.addRow("CPU Cores",heartbeatDesc.getCpuCores());
      formatter.addRow("Scribe", dataflowDescriptor.getScribe());
      formatter.addRow("Start Time", taskReport.getStartTime());
      formatter.addRow("Finish Time", taskReport.getFinishTime());
      formatter.addRow("Process Count", taskReport.getProcessCount());
      formatter.addRow("Commit Process Count", taskReport.getCommitProcessCount());
      formatter.addRow("Status", dataflowDescriptor.getStatus());
      
      b.append(formatter.getFormatText());
      
    } catch (RegistryException e) {
      e.printStackTrace();
      b.append(e.getMessage());
    }
    
    return b.toString();
  }

}
