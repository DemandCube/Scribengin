package com.neverwinterdp.scribengin.client.shell;

import com.neverwinterdp.scribengin.dataflow.test.DataflowTest;
import com.neverwinterdp.scribengin.dataflow.test.*;
import com.neverwinterdp.vm.client.shell.Command;
import com.neverwinterdp.vm.client.shell.CommandInput;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.client.shell.SubCommand;

public class DataflowTestCommand extends Command {
  
  public DataflowTestCommand() {
    add("kafka", KafkaDataflowTestSubCommand.class);
    add("hdfs", HdfsDataflowTestSubCommand.class);
    add("hdfs-kafka", HdfsToKafkaDataflowTestSubCommand.class);
    add("kafka-hdfs", KafkaToHdfsDataflowTestSubCommand.class);
    add("kafka-s3", KafkaToS3DataflowTestSubCommand.class);
  }
  static public class KafkaToHdfsDataflowTestSubCommand<T extends DataflowTest> extends SubCommand {
    Class<? extends DataflowTest> testClass ;
    
    public KafkaToHdfsDataflowTestSubCommand() {
      this.testClass = KafkaToHdfsDataflowTest.class;
    }
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginShell scribenginShell = (ScribenginShell) shell;
      DataflowTest test = testClass.newInstance() ;
      cmdInput.mapRemainArgs(test);
      test.run(scribenginShell);
    }

    @Override
    public String getDescription() {
      return "creates the sample dataflow";
    }
  }
  static public class KafkaToS3DataflowTestSubCommand<T extends DataflowTest> extends SubCommand {
    Class<? extends DataflowTest> testClass ;
    
    public KafkaToS3DataflowTestSubCommand() {
      this.testClass = KafkaToS3DataflowTest.class;
    }
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginShell scribenginShell = (ScribenginShell) shell;
      DataflowTest test = testClass.newInstance() ;
      cmdInput.mapRemainArgs(test);
      test.run(scribenginShell);
    }

    @Override
    public String getDescription() {
      return "creates the sample dataflow";
    }
  }
  static public class KafkaDataflowTestSubCommand<T extends DataflowTest> extends SubCommand {
    Class<? extends DataflowTest> testClass ;
    
    public KafkaDataflowTestSubCommand() {
      this.testClass = KafkaDataflowTest.class;
    }
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginShell scribenginShell = (ScribenginShell) shell;
      DataflowTest test = testClass.newInstance() ;
      cmdInput.mapRemainArgs(test);
      test.run(scribenginShell);
    }

    @Override
    public String getDescription() {
      return "creates the sample dataflow";
    }
  }

  static public class HdfsDataflowTestSubCommand<T extends DataflowTest> extends SubCommand {
    Class<? extends DataflowTest> testClass ;
    
    public HdfsDataflowTestSubCommand() {
      this.testClass = HDFSDataflowTest.class;
    }
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginShell scribenginShell = (ScribenginShell) shell;
      DataflowTest test = testClass.newInstance() ;
      cmdInput.mapRemainArgs(test);
      test.run(scribenginShell);
    }

    @Override
    public String getDescription() {
      return "creates the sample dataflow";
    }
  }
  static public class HdfsToKafkaDataflowTestSubCommand<T extends DataflowTest> extends SubCommand {
    Class<? extends DataflowTest> testClass ;
    
    public HdfsToKafkaDataflowTestSubCommand() {
      this.testClass = HDFSToKafkaDataflowTest.class;
    }
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginShell scribenginShell = (ScribenginShell) shell;
      DataflowTest test = testClass.newInstance() ;
      cmdInput.mapRemainArgs(test);
      test.run(scribenginShell);
    }

    @Override
    public String getDescription() {
      return "creates the sample dataflow";
    }
  }


  
  @Override
  public String getDescription() {
    return "a sample dataflow";
  }
  
}
