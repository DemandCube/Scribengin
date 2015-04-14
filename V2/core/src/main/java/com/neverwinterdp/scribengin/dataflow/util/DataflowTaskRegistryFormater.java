package com.neverwinterdp.scribengin.dataflow.util;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.util.NodeFormatter;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskReport;
import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.util.text.TabularFormater;

/**
 * @author Tuan
 */
public class DataflowTaskRegistryFormater extends NodeFormatter {
  private Node taskDescriptorNode ;
  
  public DataflowTaskRegistryFormater(Node taskNode) {
    this.taskDescriptorNode = taskNode;
  }
  
  @Override
  public String getFormattedText() {
    StringBuilder b = new StringBuilder() ;
    try {
      if(!taskDescriptorNode.exists()){
        return "Dataflow task activityNode is already deleted or moved to the history";
      }
      
      DataflowTaskDescriptor dflDescriptor = taskDescriptorNode.getDataAs(DataflowTaskDescriptor.class);
      DataflowTaskReport     dflTaskReport = taskDescriptorNode.getChild("report").getDataAs(DataflowTaskReport.class);
      
      //TODO: try to make the print out in one line with the format property = value, property = value.... 
      TabularFormater taskFt = new TabularFormater("Property", "Value");
      taskFt.addRow("Dataflow Task Descriptor", "");
      taskFt.addRow("  Dataflow Task Id", dflDescriptor.getId());
      taskFt.addRow("  Status", dflDescriptor.getStatus());
      taskFt.addRow("  Registry Path", taskDescriptorNode.getPath());
      taskFt.addRow("Dataflow Task Report", "");
      taskFt.addRow("  Scribe", dflDescriptor.getScribe());
      taskFt.addRow("  Start Time", DateUtil.asCompactDateTime(dflTaskReport.getStartTime()));
      taskFt.addRow("  Finish Time", DateUtil.asCompactDateTime(dflTaskReport.getFinishTime()));
      taskFt.addRow("  Process Count", dflTaskReport.getProcessCount());
      taskFt.addRow("  Commit Process Count", dflTaskReport.getCommitProcessCount());
      b.append(taskFt.getFormatText());
    } catch (Exception e) {
      e.printStackTrace();
      b.append(e.getMessage());
    }
    
    return b.toString();
  }

}
