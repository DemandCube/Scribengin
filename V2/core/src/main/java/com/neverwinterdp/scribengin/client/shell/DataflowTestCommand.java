package com.neverwinterdp.scribengin.client.shell;

import com.neverwinterdp.scribengin.dataflow.test.DataflowCommandStartStopResumeTest;
import com.neverwinterdp.scribengin.dataflow.test.DataflowCommandTest;
import com.neverwinterdp.scribengin.dataflow.test.DataflowRandomServerFailureTest;
import com.neverwinterdp.scribengin.dataflow.test.DataflowTest;
import com.neverwinterdp.scribengin.dataflow.test.HDFSDataflowTest;
import com.neverwinterdp.scribengin.dataflow.test.HDFSToKafkaDataflowTest;
import com.neverwinterdp.scribengin.dataflow.test.KafkaDataflowTest;
import com.neverwinterdp.scribengin.dataflow.test.KafkaToHdfsDataflowTest;
import com.neverwinterdp.scribengin.dataflow.test.KafkaToS3DataflowTest;
import com.neverwinterdp.vm.client.shell.Command;
import com.neverwinterdp.vm.client.shell.CommandInput;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.client.shell.SubCommand;

public class DataflowTestCommand extends Command {
  
  public DataflowTestCommand() {
    add(KafkaDataflowTest.TEST_NAME, KafkaDataflowTestSubCommand.class);

    //TODO: fix the other sub command, you should realize that you have a lot similar and copy/paste code.
    add(HDFSDataflowTest.TEST_NAME, HdfsDataflowTestSubCommand.class);
    add(HDFSToKafkaDataflowTest.TEST_NAME, HdfsToKafkaDataflowTestSubCommand.class);
    add(KafkaToHdfsDataflowTest.TEST_NAME, KafkaToHdfsDataflowTestSubCommand.class);

    add(DataflowCommandStartStopResumeTest.TEST_NAME, StartStopResumeSubcommand.class);
    add(DataflowRandomServerFailureTest.TEST_NAME, RandomServerFailureSubcommand.class);
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
  
  static public class RandomServerFailureSubcommand<T extends DataflowCommandTest> extends DataflowCommandTestSubCommand<T> {
    public RandomServerFailureSubcommand() {
      super(DataflowRandomServerFailureTest.class);
    }
  }
  
  
  @Override
  public String getDescription() {
    return "a sample dataflow";
  }
  
}
