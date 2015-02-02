package com.neverwinterdp.scribengin.client.shell;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.DataflowClient;
import com.neverwinterdp.scribengin.dataflow.builder.HelloHDFSDataflowBuilder;
import com.neverwinterdp.scribengin.dataflow.builder.HelloKafkaDataflowBuilder;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.util.IOUtil;
import com.neverwinterdp.vm.HadoopProperties;
import com.neverwinterdp.vm.client.shell.Command;
import com.neverwinterdp.vm.client.shell.CommandInput;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.client.shell.SubCommand;

public class DataflowCommand extends Command {
  public DataflowCommand() {
    add("submit", new Submit()) ;
    add("hello",  new Hello()) ;
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
        String dataDir = "/data" ;
        HadoopProperties hadoopProperties = shell.attribute(HadoopProperties.class);
        FileSystem fs = FileSystem.get(hadoopProperties.getConfiguration());
        if(fs.exists(new Path(dataDir))) {
          fs.delete(new Path(dataDir), true) ;
        }
        HelloHDFSDataflowBuilder hdfDataflowBuilder = new HelloHDFSDataflowBuilder(scribenginClient, fs, dataDir);
        hdfDataflowBuilder.createSource(15, 3, 5);
        
        DataflowClient dataflowClient = new DataflowClient(scribenginClient);
        String dataflowJson = IOUtil.getFileContentAsString(descriptor) ;
        System.out.println("Dataflow JSON:");
        System.out.println(dataflowJson);
        ScribenginWaitingEventListener eventListener = dataflowClient.submit(dataflowPath, dataflowJson);
        System.out.println("Submited.................");
        eventListener.waitForEvents(60000); 
        System.out.println("Finish wait for event..........");
      } catch(Exception ex) {
        ex.printStackTrace();
      } finally {
        Thread.sleep(3000);
        shell.execute("vm list");
        shell.execute("registry dump --path /");
      }
    }
  }
  
  static public class Hello extends SubCommand {
    @Parameter(names = "--submit", description = "Launch the submit dataflow(hdfs, kafka)")
    private String submit ;
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginShell scribenginShell = (ScribenginShell) shell;
      if("hdfs".equals(submit)) {
        submitHdfsDataflow(scribenginShell);
      } else if("kafka".equals(submit)) {
        submitKafkaDataflow(scribenginShell);
      }
    }
    
    void submitHdfsDataflow(ScribenginShell shell) throws Exception {
      HadoopProperties hadoopProperties = shell.attribute(HadoopProperties.class);
      String dataDir = "/data" ;
      FileSystem fs = FileSystem.get(hadoopProperties.getConfiguration());
      if(fs.exists(new Path(dataDir))) {
        fs.delete(new Path(dataDir), true) ;
      }
      HelloHDFSDataflowBuilder dataflowBuilder = 
        new HelloHDFSDataflowBuilder(shell.getScribenginClient(), fs, dataDir);
      dataflowBuilder.setNumOfWorkers(1);
      dataflowBuilder.setNumOfExecutorPerWorker(2);
      dataflowBuilder.createSource(15, 3, 5);

      ScribenginWaitingEventListener sribenginAssert = dataflowBuilder.submit();
      sribenginAssert.waitForEvents(90000);
    }
    
    void submitKafkaDataflow(ScribenginShell shell) throws Exception {
      HelloKafkaDataflowBuilder kafkaDataflowBuilder = 
          new HelloKafkaDataflowBuilder(shell.getScribenginClient());
      kafkaDataflowBuilder.createSource(5, 10);
      ScribenginWaitingEventListener sribenginAssert = kafkaDataflowBuilder.submit();
      sribenginAssert.waitForEvents(60000);
    }
  }
}
