package com.neverwinterdp.scribengin.client.shell;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.scribengin.dataflow.builder.HelloHDFSDataflowBuilder;
import com.neverwinterdp.scribengin.dataflow.builder.HelloKafkaDataflowBuilder;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.vm.client.shell.Command;
import com.neverwinterdp.vm.client.shell.CommandInput;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.client.shell.SubCommand;

public class DataflowCommand extends Command {
  public DataflowCommand() {
    add("hello", new Hello()) ;
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
