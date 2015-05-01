package com.neverwinterdp.scribengin.client.shell;

import java.util.LinkedList;
import java.util.List;

import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityStep;
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
      TabularFormater formater = new TabularFormater("Id", "Name", "App Home", "Workers", "Executor Per Worker");
      formater.setIndent("  ");
      for (int i = 0; i < descriptors.size(); i++) {
        DataflowDescriptor descriptor = descriptors.get(i);
        formater.addRow(
            descriptor.getId(),
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
        new TabularFormater("dataflowName", "CPU Cores", "Memory", "Path", "is Leader");
      formater.setIndent("  ");
      for (VMDescriptor descriptor : descriptors) {
        formater.addRow(descriptor.getVmConfig().getName(),
            descriptor.getCpuCores(),
            descriptor.getMemory(),
            descriptor.getRegistryPath(),
            descriptor.getRegistryPath().equals(leaderPath));
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
      int stringLengthLimit = 50;
      
      for (DataflowTaskDescriptor descriptor : descriptors) {
        String scribeName = descriptor.getScribe();
        if(scribeName.length() > stringLengthLimit){
          scribeName = "..." +
                       descriptor.getScribe().
                       substring(descriptor.getScribe().length()-stringLengthLimit, 
                           descriptor.getScribe().length());
        }
        formater.addRow(
            descriptor.getId(),
            scribeName,
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
        String finishTime="";
        if( descriptor.getFinishTime() > 0 ){
          finishTime = DateUtil.asCompactDateTime(descriptor.getFinishTime());
        }
        
        formater.addRow(
            descriptor.getProcessCount(),
            descriptor.getCommitProcessCount(),
            DateUtil.asCompactDateTime(descriptor.getStartTime()),
            finishTime
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
  
  public static class ActivityFormatter{
    private List<ActivityStep> activitySteps;
    private Activity activity;
    boolean verbose;

    public ActivityFormatter(Activity activity, List<ActivityStep> activitySteps, boolean verbose) {
      this.activity = activity;
      this.activitySteps = activitySteps;
      this.verbose = verbose;
    }
    
    public String format() {
      return format("");
    }
    
    public String format(String indent) {
      activity.getLogs();
      TabularFormater stepFormatter = 
          new TabularFormater("Step ID", "Type", "Max Retries", "Try", "Exec Time", "Status", "Description");
      
      //Activity Logs
      TabularFormater activityLogsFormatter = new TabularFormater(activity.getId()+" Activity Logs");
      activityLogsFormatter.setIndent(indent+"  ");
      List<String> activityLogs = activity.getLogs();
      if(activityLogs != null){
        for(String log: activityLogs){
          Object[] cells = {log};
          activityLogsFormatter.addRow(cells);
        }
      } else{
        Object[] cells = {"No logs available"};
        activityLogsFormatter.addRow(cells);
      }
      
      //Steps
      stepFormatter.setIndent(indent);
      stepFormatter.setTitle(
                        "Activity: "+
                        "ID = " + this.activity.getId() + ", "+
                        "Type = " + this.activity.getType() + ", "+
                        "Description = " + this.activity.getDescription());
      
      
      LinkedList<TabularFormater> stepLogsFormatterList = new LinkedList<TabularFormater>();
      
      for(ActivityStep step : activitySteps) {
        Object[] cells = {
          step.getId(), step.getType(), step.getMaxRetries(), step.getTryCount(), 
          step.getExecuteTime(), step.getStatus(), step.getDescription()
        } ;
        stepFormatter.addRow(cells);
        
        //Step logs
        TabularFormater stepLogsFormatter = new TabularFormater(step.getId()+" Step Logs, Status:"+step.getStatus());
        stepLogsFormatter.setIndent(indent+"  ");
        List<String> stepLogs = step.getLogs();
        if(stepLogs != null){
          for(String log: stepLogs){
            Object[] cell = {  log };
            stepLogsFormatter.addRow(cell);
          }
        }
        else{
          Object[] cell = {  "No logs available" };
          stepLogsFormatter.addRow(cell);
        }
        
        stepLogsFormatterList.add(stepLogsFormatter);
      }
      
      if(verbose){
        String result = stepFormatter.getFormatText()+"\n    Activity Logs\n"+
            activityLogsFormatter.getFormatText()+"\n    Activity Step Logs\n";
        for(TabularFormater f: stepLogsFormatterList){
          result+=f.getFormatText()+"\n";
        }
        return result;
      }
      else{
        return stepFormatter.getFormatText();
      }
    }
  }
  
}