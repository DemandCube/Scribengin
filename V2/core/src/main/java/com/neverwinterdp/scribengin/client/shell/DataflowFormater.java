package com.neverwinterdp.scribengin.client.shell;

import java.util.List;

import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskReport;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowTaskExecutorDescriptor;
import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.util.text.TabularFormater;

public class DataflowFormater {
  static public String formatDescriptor(String title, List<DataflowTaskDescriptor> descriptors) {
    TabularFormater formater = new TabularFormater("Id", "Data Processor", "Status");
    formater.setIndent("  ");
    for(int i = 0; i < descriptors.size(); i++) {
      DataflowTaskDescriptor descriptor = descriptors.get(i) ;
      formater.addRow(
          descriptor.getId(), 
          descriptor.getDataProcessor(),
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