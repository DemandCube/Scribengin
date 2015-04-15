package com.neverwinterdp.scribengin.dataflow.util;

import java.util.List;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.util.NodeFormatter;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskReport;
import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.VMDescriptor;

/**
 * The goal of this class is to print out all and in detail of the information of the triggered task and the status 
 * of the other tasks
 * @author Tuan
 */
public class DataflowTaskRegistryDetailedFormater extends NodeFormatter {
  private Node taskDescriptorNode ;
  
  public DataflowTaskRegistryDetailedFormater(Node taskNode) {
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
      
      VMDescriptor workerDescriptor = null ;
      
      Node workerHeartbeatNode = 
        taskDescriptorNode.getParentNode().
        getParentNode().getDescendant("executors/assigned/" + taskDescriptorNode.getName() + "/heartbeat");
      if(workerHeartbeatNode.exists()) {
        workerDescriptor = workerHeartbeatNode.getDataAs(VMDescriptor.class) ;
      }
      else{
        
      }

      
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
      taskFt.addRow("Worker", "");
      if(workerDescriptor != null) { 
        taskFt.addRow("  Id", workerDescriptor.getId());
        taskFt.addRow("  Registry Path", workerDescriptor.getStoredPath());
      } else{
        DataflowTaskDescriptor.Status status = dflDescriptor.getStatus();
        if(status != DataflowTaskDescriptor.Status.SUSPENDED && status != DataflowTaskDescriptor.Status.TERMINATED) {
          taskFt.addRow("  Status", "FAILED");
        } else if(status == DataflowTaskDescriptor.Status.TERMINATED) {
          taskFt.addRow("  Status", "FINISHED");
        } else{
          taskFt.addRow("  Status", status);
        }
      }
      b.append(taskFt.getFormatText());
      
      Node taskDescriptorsNode = taskDescriptorNode.getParentNode();
      List<DataflowTaskDescriptor> taskDescriptors = 
        taskDescriptorsNode.getChildrenAs(DataflowTaskDescriptor.class, DataflowRegistry.TASK_DESCRIPTOR_DATA_MAPPER);
      TabularFormater tasksFt = new TabularFormater("Id", "Status", "Registry Path");
      tasksFt.setTitle("Dataflow Tasks");
      for(int i = 0; i < taskDescriptors.size(); i++) {
        DataflowTaskDescriptor descriptor = taskDescriptors.get(i) ;
        tasksFt.addRow(descriptor.getId(), descriptor.getStatus(), descriptor.getStoredPath());
      }
      b.append("\n");
      b.append(tasksFt.getFormatText());
    } catch (Exception e) {
      e.printStackTrace();
      b.append(e.getMessage());
    }
    
    return b.toString();
  }

}
