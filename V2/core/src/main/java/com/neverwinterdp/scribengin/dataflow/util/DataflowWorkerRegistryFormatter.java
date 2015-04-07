package com.neverwinterdp.scribengin.dataflow.util;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.util.NodeFormatter;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowTaskExecutorDescriptor;

import java.util.List;

public class DataflowWorkerRegistryFormatter extends NodeFormatter {
  private Node vmNode ;
  
  public DataflowWorkerRegistryFormatter(Node vmNode) {
    this.vmNode = vmNode;
  }
  
  @Override
  public String getFormattedText() {
    StringBuilder b = new StringBuilder() ;
    b.append("------------------------------------------------------------------------\n");
    try {
      //DataflowTaskExecutorDescriptor executorDescriptor = vmNode.getDataAs(DataflowTaskExecutorDescriptor.class);
      
      b.append(new String(vmNode.getData()));
      b.append("\n");
      List<String> execs = vmNode.getChild("executors").getChildren();
      for(String exec : execs){
        DataflowTaskExecutorDescriptor desc = vmNode.getChild("executors").getChild(exec).getDataAs(DataflowTaskExecutorDescriptor.class);
        b.append("Executor "+execs+" :\n");
        b.append("   ID                  : ");
        b.append(desc.getId());
        b.append("\n");
        b.append("   Status              : ");
        b.append(desc.getStatus());
        b.append("\n");
        b.append("   TaskIDs             : ");
        b.append(desc.getAssignedTaskIds().toString());
        b.append("\n");
      }
    } catch (RegistryException e) {
      e.printStackTrace();
      b.append(e.getMessage());
    }
    b.append("------------------------------------------------------------------------\n");
    return b.toString();
  }
}
