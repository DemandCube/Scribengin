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
    List<DataflowDescriptor> descriptors;

    public DataflowList(List<DataflowDescriptor> descriptors) {
      this.descriptors = descriptors;
    }

    public String format(String title) {
      return format(title, "");
    }

    public String format(String title, String ident) {
      TabularFormater formater = new TabularFormater("Name", "App Home", "Workers", "Executor Per Worker");
      formater.setIndent("  ");
      for (int i = 0; i < descriptors.size(); i++) {
        DataflowDescriptor descriptor = descriptors.get(i);
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

  static public class VmList {

    private List<VMDescriptor> descriptors;
    String leaderPath;

    public VmList(List<VMDescriptor> descriptors, String leaderPath) {
      this.descriptors = descriptors;
      this.leaderPath = leaderPath;
    }

    public String format(String title) {
      return format(title, "");
    }

    public String format(String title, String ident) {
      TabularFormater formater = 
        new TabularFormater("name", "CPU Cores", "Memory", "Path", "is Leader");
      formater.setIndent("  ");
      for (VMDescriptor descriptor : descriptors) {
        formater.addRow(descriptor.getVmConfig().getName(),
            descriptor.getCpuCores(),
            descriptor.getMemory(),
            descriptor.getStoredPath(),
            descriptor.getStoredPath().equals(leaderPath));
      }
      formater.setTitle(title);
      return formater.getFormatText();
    }

  }

  public static class DataFlowTaskDescriptorList {

    private List<DataflowTaskDescriptor> descriptors;

    public DataFlowTaskDescriptorList(List<DataflowTaskDescriptor> descriptors) {
      this.descriptors = descriptors;
    }

    public String format(String title) {
      return format(title, "");
    }

    public String format(String title, String ident) {
      TabularFormater formater = new TabularFormater("Id", "Scribe", "Status");
      formater.setIndent("  ");
      for (DataflowTaskDescriptor descriptor : descriptors) {
        formater.addRow(
            descriptor.getId(),
            descriptor.getScribe(),
            descriptor.getStatus()
            );
      }
      formater.setTitle(title);
      return formater.getFormatText();
    }

  }

  public static class DataflowTaskReportList {

    private List<DataflowTaskReport> descriptors;

    public DataflowTaskReportList(List<DataflowTaskReport> descriptors) {
      this.descriptors = descriptors;
    }

    public String format(String title, String indent) {
      TabularFormater formater = new TabularFormater("Process Count", "Commit Count", "Start Time",
          "Finish Time");
      formater.setIndent(indent);
      for (DataflowTaskReport descriptor : descriptors) {
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
  }

  public static class ExecutorList {

    private List<DataflowTaskExecutorDescriptor> descriptors;

    public ExecutorList(List<DataflowTaskExecutorDescriptor> descriptors) {
      this.descriptors = descriptors;
    }

    public String format(String title) {
      return format(title, "");
    }

    public String format(String title, String indent) {
      TabularFormater formater = new TabularFormater("Id", "Status", "Assigned Tasks");
      formater.setIndent(indent);
      for (DataflowTaskExecutorDescriptor descriptor : descriptors) {
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
}