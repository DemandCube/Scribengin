package com.neverwinterdp.scribengin.client.shell;

import com.neverwinterdp.scribengin.dataflow.test.*;
import com.neverwinterdp.vm.client.shell.Command;
import com.neverwinterdp.vm.client.shell.CommandInput;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.client.shell.SubCommand;

public class DataflowTestCommand extends Command {
  
  public DataflowTestCommand() {
    add(KafkaDataflowTest.TEST_NAME, KafkaDataflowTestSubCommand.class);
    
    //TODO: fix the name as KafkaDataflowTest.TEST_NAME, in unit test , use XYZDataflowTest.TEST_NAME as well,
    //so you can go to the test code easily
    //TODO: fix the other sub command, you should realize that you have a lot similar and copy/paste code.
    add("hdfs", HdfsDataflowTestSubCommand.class);
    add("hdfs-kafka", HdfsToKafkaDataflowTestSubCommand.class);
    add("kafka-hdfs", KafkaToHdfsDataflowTestSubCommand.class);
    add("kafka-s3", KafkaToS3DataflowTestSubCommand.class);
    
    add(DataflowCommandStartStopResumeTest.TEST_NAME, StartStopResumeSubcommand.class);
  }
  
  static public class DataflowTestSubCommand<T extends DataflowTest> extends SubCommand {
    Class<? extends DataflowTest> testClass ;
    
    public DataflowTestSubCommand(Class<? extends DataflowTest> testClass) {
      this.testClass = testClass;
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
  
  static public class KafkaDataflowTestSubCommand<T extends DataflowTest> extends DataflowTestSubCommand<T> {
    public KafkaDataflowTestSubCommand() {
      super(KafkaDataflowTest.class);
    }
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
  
  static public class HdfsDataflowTestSubCommand<T extends DataflowTest> extends SubCommand {
    Class<? extends DataflowTest> testClass ;
    
    public HdfsDataflowTestSubCommand() {
      this.testClass = HDFSDataflowTest.class;
    }
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      System.err.println("how abut here.");
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
  
  static public class DataflowCommandTestSubCommand<T extends DataflowCommandTest> extends SubCommand {
    Class<? extends DataflowCommandTest> testClass ;
    
    public DataflowCommandTestSubCommand(Class<? extends DataflowCommandTest> testClass) {
      this.testClass = testClass;
    }
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginShell scribenginShell = (ScribenginShell) shell;
      DataflowCommandTest test = testClass.newInstance() ;
      cmdInput.mapRemainArgs(test);
      test.run(scribenginShell);
    }

    @Override
    public String getDescription() {
      return "creates the sample dataflow";
    }
  }
  
  static public class StartStopResumeSubcommand<T extends DataflowCommandTest> extends DataflowCommandTestSubCommand<T> {
    public StartStopResumeSubcommand() {
      super(DataflowCommandStartStopResumeTest.class);
    }
  }
  
  @Override
  public String getDescription() {
    return "a sample dataflow";
  }
  
}
