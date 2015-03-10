package com.neverwinterdp.scribengin.client.shell;

import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskReport;
import com.neverwinterdp.scribengin.dataflow.test.HelloHDFSDataflowBuilder;
import com.neverwinterdp.scribengin.dataflow.test.HelloKafkaDataflowBuilder;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowTaskExecutorDescriptor;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.scribengin.hdfs.HDFSSourceGenerator;
import com.neverwinterdp.scribengin.kafka.KafkaSourceGenerator;
import com.neverwinterdp.util.IOUtil;
import com.neverwinterdp.vm.HadoopProperties;
import com.neverwinterdp.vm.client.shell.Command;
import com.neverwinterdp.vm.client.shell.CommandInput;
import com.neverwinterdp.vm.client.shell.Console;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.client.shell.SubCommand;

public class DataflowCommand extends Command {
  public DataflowCommand() {
    add("info",   Info.class) ;
    add("submit", Submit.class) ;
    add("hdfs",   Hdfs.class) ;
    add("kafka",  Kafka.class) ;
  }
  
  static public class Info extends SubCommand {
    @Parameter(names = "--running", description = "The running dataflow name")
    private String running ;
    
    @Parameter(names = "--history", description = "The history dataflow id")
    private String history ;
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginShell scribenginShell = (ScribenginShell) shell;
      ScribenginClient scribenginClient= scribenginShell.getScribenginClient();
      DataflowRegistry dRegistry = null;
      if(running != null) {
        dRegistry = scribenginClient.getRunningDataflowRegistry(running);
      } else if(history != null) {
        dRegistry = scribenginClient.getHistoryDataflowRegistry(history);
      }
      Console console = shell.console();
      console.h1("Dataflow " + dRegistry.getDataflowPath());
      console.println("\nTasks:\n");
      List<DataflowTaskDescriptor> taskDescriptors = dRegistry.getTaskDescriptors();
      Formater.DataFlowTaskDescriptorList taskList = new Formater.DataFlowTaskDescriptorList(taskDescriptors);
      console.println(taskList.format("All Tasks"));
      List<DataflowTaskReport> taskReports = dRegistry.getTaskReports(taskDescriptors) ;
      Formater.DataflowTaskReportList reportList = new Formater.DataflowTaskReportList(taskReports);
      console.print(reportList.format("Report", "  "));
      
      console.println("Workers:\n");
      List<String> workers = dRegistry.getWorkerNames();
      for(String worker : workers) {
        List<DataflowTaskExecutorDescriptor> descriptors = dRegistry.getExecutors(worker);
        console.println("\n  Worker: " + worker + "\n");
        Formater.ExecutorList executorList = new Formater.ExecutorList(descriptors);
        console.println(executorList.format("Executors", "    "));
      }
    }

    @Override
    public String getDescription() {
      return "display more info about dataflows";
    }
  }
  
  static public class Submit extends SubCommand {
    @Parameter(names = "--descriptor", required = true, description = "The dataflow descriptor path in the json format")
    private String descriptor ;
    
    @Parameter(names = "--deploy", description = "The dataflow path to deploy")
    private String dataflowPath ;
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginShell scribenginShell = (ScribenginShell) shell;
      ScribenginClient scribenginClient= scribenginShell.getScribenginClient();
      try {
        String dataflowJson = IOUtil.getFileContentAsString(descriptor) ;
        System.out.println("Dataflow JSON:");
        System.out.println(dataflowJson);
        ScribenginWaitingEventListener eventListener = scribenginClient.submit(dataflowPath, dataflowJson);
        System.out.println("Submited.................");
        eventListener.waitForEvents(60000); 
        System.out.println("Finish wait for event..........");
      } catch(Exception ex) {
        ex.printStackTrace();
      } finally {
        Thread.sleep(3000);
        shell.execute("vm info");
        shell.execute("registry dump --path /");
      }
    }

    @Override
    public String getDescription() {
      return "submit a dataflow";
    }
  }
  
  static public class Kafka extends SubCommand {
    @Parameter(names = "--create-source", description = "Create kafka source")
    private boolean createSource ;
    
    @Parameter(names = "--submit", description = "Launch the submit dataflow(hdfs, kafka)")
    private boolean submit ;
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginShell scribenginShell = (ScribenginShell) shell;
      HelloKafkaDataflowBuilder kafkaDataflowBuilder = 
          new HelloKafkaDataflowBuilder(scribenginShell.getScribenginClient());
      
      if(submit || createSource) {
        String zkConnect = scribenginShell.getScribenginClient().getRegistry().getRegistryConfig().getConnect();
        KafkaSourceGenerator generator = new KafkaSourceGenerator("hello", zkConnect);
        generator.generateAndWait("hello.source");
      }
      
      if(submit) {
        ScribenginWaitingEventListener sribenginAssert = kafkaDataflowBuilder.submit();
        sribenginAssert.waitForEvents(60000);
      }
    }

    @Override
    public String getDescription() {
      return "submit a kafka dataflow";
    }
  }
  
  static public class Hdfs extends SubCommand {
    @Parameter(names = "--data-dir", description = "Submit hello hdfs dataflow")
    private String dataDir = "/data";
    
    @Parameter(names = "--create-source", description = "Submit hello hdfs dataflow")
    private boolean createSource ;
    
    @Parameter(names = "--submit", description = "Submit hello hdfs dataflow")
    private boolean submit ;
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginShell scribenginShell = (ScribenginShell) shell;
      HadoopProperties hadoopProperties = shell.attribute(HadoopProperties.class);
      FileSystem fs = FileSystem.get(hadoopProperties.getConfiguration());
      if(fs.exists(new Path(dataDir))) {
        fs.delete(new Path(dataDir), true) ;
      }
      HelloHDFSDataflowBuilder dataflowBuilder = new HelloHDFSDataflowBuilder(scribenginShell.getScribenginClient(), dataDir);
      dataflowBuilder.setNumOfWorkers(1);
      dataflowBuilder.setNumOfExecutorPerWorker(2);
      if(submit || createSource) {
        new HDFSSourceGenerator().generateSource(fs, dataDir + "/source");
      }
      if(submit) {
        ScribenginWaitingEventListener sribenginAssert = dataflowBuilder.submit();
        sribenginAssert.waitForEvents(90000);
      }
    }

    @Override
    public String getDescription() {
      return "submit a HDFS dataflow";
    }
  }

  @Override
  public String getDescription() {
        return "commands for interacting with dataflows";
  }
}
