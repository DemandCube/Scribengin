package com.neverwinterdp.scribengin.client.shell;

import java.util.List;

import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskReport;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowTaskExecutorDescriptor;
import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.VMDescriptor;

public class Formater {
  static public class DataflowList {
    List<DataflowDescriptor> descriptors ;
    
    public DataflowList(List<DataflowDescriptor> descriptors) {
      this.descriptors = descriptors;
    }
    
    public String format(String title) { return format(title, ""); }
    
    public String format(String title, String ident) {
      TabularFormater formater = new TabularFormater("Name", "App Home", "Workers", "Executor Per Worker");
      formater.setIndent("  ");
      for(int i = 0; i < descriptors.size(); i++) {
        DataflowDescriptor descriptor = descriptors.get(i) ;
        formater.addRow(
            descriptor.getName(), 
            descriptor.getDataflowAppHome(),
            descriptor.getNumberOfWorkers(),
            descriptor.getNumberOfExecutorsPerWorker()
            );
      }
      formater.setTitle(title);
      return formater.getFormatText();
    }
  }

  //TODO: 
  // 1. Check the above formater class, use the inner static class and format method:
  // 2. Help me to search and replace the other format method in this class
  public static String format(String title, List<VMDescriptor> vmDescriptors, String leaderPath) {
    TabularFormater formater = new TabularFormater("name", "CPU Cores", "Memory", "Path", "is Leader");
    formater.setIndent("  ");
    for (VMDescriptor descriptor : vmDescriptors) {
      formater.addRow(descriptor.getVmConfig().getName(), 
          descriptor.getCpuCores(),
          descriptor.getMemory(), 
          descriptor.getStoredPath(), 
          descriptor.getStoredPath().equals(leaderPath));
    }
    formater.setTitle(title);
    return formater.getFormatText();
  }
  
  static public String formatDescriptor(String title, List<DataflowTaskDescriptor> descriptors) {
    TabularFormater formater = new TabularFormater("Id", "Scribe", "Status");
    formater.setIndent("  ");
    for(int i = 0; i < descriptors.size(); i++) {
      DataflowTaskDescriptor descriptor = descriptors.get(i) ;
      formater.addRow(
          descriptor.getId(), 
          descriptor.getScribe(),
          descriptor.getStatus()
      );
    }
    formater.setTitle(title);
    return formater.getFormatText();
  }
  
  static public String formatReport(String title, List<DataflowTaskReport> descriptors, String indent) {
    TabularFormater formater = new TabularFormater("Process Count", "Commit Count", "Start Time", "Finish Time");
    formater.setIndent(indent);
    for(int i = 0; i < descriptors.size(); i++) {
      DataflowTaskReport descriptor = descriptors.get(i) ;
      formater.addRow(
        descriptor.getProcessCount(),
        descriptor.getCommitProcessCount(),
        DateUtil.asCompactDateTime(descriptor.getStartTime()),
        DateUtil.asCompactDateTime(descriptor.getFinishTime())
      );
    }
    formater.setTitle(title);
    return formater.getFormatText();
  }
  
  static public String formatExecutor(String title, List<DataflowTaskExecutorDescriptor> descriptors, String indent) {
    TabularFormater formater = new TabularFormater("Id", "Status", "Assigned Tasks");
    formater.setIndent(indent);
    for(int i = 0; i < descriptors.size(); i++) {
      DataflowTaskExecutorDescriptor descriptor = descriptors.get(i) ;
      formater.addRow(
          descriptor.getId(), 
          descriptor.getStatus(),
          descriptor.getAssignedTaskIds()
      );
    }
    formater.setTitle(title);
    return formater.getFormatText();
  }
}