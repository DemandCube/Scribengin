package com.neverwinterdp.scribengin.dataflow.util;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.util.NodeFormatter;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskReport;
import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.VMDescriptor;


public class DataflowTaskRegistryFormatter extends NodeFormatter {
  private Node taskDescriptorNode ;
  
  public DataflowTaskRegistryFormatter(Node taskNode) {
    this.taskDescriptorNode = taskNode;
  }
  
  @Override
  public String getFormattedText() {
    StringBuilder b = new StringBuilder() ;
    try {
      if(!taskDescriptorNode.exists()){
        return "Dataflow task node is already deleted or moved to the history";
      }
      
      DataflowTaskDescriptor dflDescriptor = taskDescriptorNode.getDataAs(DataflowTaskDescriptor.class);
      DataflowTaskReport     dflTaskReport = taskDescriptorNode.getChild("report").getDataAs(DataflowTaskReport.class);
      
      VMDescriptor workerDescriptor = null ;
      
      Node workerHeartbeatNode = 
        taskDescriptorNode.getParentNode().
        getParentNode().getDescendant("executors/assigned/" + taskDescriptorNode.getName() + "/heartbeat");
      if(workerHeartbeatNode.exists()) {
        workerDescriptor = workerHeartbeatNode.getDataAs(VMDescriptor.class) ;
      }
      
      TabularFormater formatter = new TabularFormater("Property", "Value");
      
      formatter.addRow("Dataflow Task Descriptor", "");
      formatter.addRow("  Dataflow Task Id", dflDescriptor.getId());
      formatter.addRow("  Status", dflDescriptor.getStatus());
      formatter.addRow("  Registry Path", taskDescriptorNode.getPath());
      formatter.addRow("Dataflow Task Report", "");
      formatter.addRow("  Scribe", dflDescriptor.getScribe());
      formatter.addRow("  Start Time", DateUtil.asCompactDateTime(dflTaskReport.getStartTime()));
      formatter.addRow("  Finish Time", DateUtil.asCompactDateTime(dflTaskReport.getFinishTime()));
      formatter.addRow("  Process Count", dflTaskReport.getProcessCount());
      formatter.addRow("  Commit Process Count", dflTaskReport.getCommitProcessCount());
      formatter.addRow("Worker", "");
      if(workerDescriptor != null) { 
        formatter.addRow("  Id", workerDescriptor.getId());
        formatter.addRow("  Registry Path", workerDescriptor.getStoredPath());
      }
      b.append(formatter.getFormatText());
      
    } catch (Exception e) {
      e.printStackTrace();
      b.append(e.getMessage());
    }
    
    return b.toString();
  }

}
