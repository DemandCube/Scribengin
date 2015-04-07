package com.neverwinterdp.scribengin.dataflow.util;

import java.util.Map;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.util.NodeFormatter;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskReport;

public class DataflowTaskRegistryFormatter extends NodeFormatter {
  private Node vmNode ;
  
  public DataflowTaskRegistryFormatter(Node vmNode) {
    this.vmNode = vmNode;
  }
  
  @Override
  public String getFormattedText() {
    StringBuilder b = new StringBuilder() ;
    try {
      DataflowTaskDescriptor dataflowDescriptor = vmNode.getDataAs(DataflowTaskDescriptor.class);
      DataflowTaskReport     taskReport         = vmNode.getChild("report").getDataAs(DataflowTaskReport.class);
      
      b.append("------------------------------------------------------------------------\n");
      b.append("ID                    : ");
      b.append(dataflowDescriptor.getId());
      b.append("\n");
      
      b.append("Scribe                : ");
      b.append(dataflowDescriptor.getScribe());
      b.append("\n");
      
      b.append("Stored Path           : ");
      b.append(dataflowDescriptor.getStoredPath());
      b.append("\n");
      
      b.append("Start Time            : ");
      b.append(taskReport.getStartTime());
      b.append("\n");
      
      b.append("Finish Time           : ");
      b.append(taskReport.getFinishTime());
      b.append("\n");
      
      b.append("Process Count         : ");
      b.append(taskReport.getProcessCount());
      b.append("\n");
      
      b.append("Commit Process Count  : ");
      b.append(taskReport.getCommitProcessCount());
      b.append("\n");
      
      
      b.append("Status                : ");
      b.append(dataflowDescriptor.getStatus());
      b.append("\n");
      
    } catch (RegistryException e) {
      e.printStackTrace();
      b.append(e.getMessage());
    }
    
    return b.toString();
  }

}
