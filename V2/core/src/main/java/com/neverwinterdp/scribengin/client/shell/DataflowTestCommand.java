package com.neverwinterdp.scribengin.client.shell;

import com.neverwinterdp.scribengin.dataflow.test.DataflowTest;
import com.neverwinterdp.scribengin.dataflow.test.HdfsDataflowTest;
import com.neverwinterdp.scribengin.dataflow.test.KafkaDataflowTest;
import com.neverwinterdp.vm.client.shell.Command;
import com.neverwinterdp.vm.client.shell.CommandInput;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.client.shell.SubCommand;

public class DataflowTestCommand extends Command {
  
  public DataflowTestCommand() {
    add("kafka", KafkaDataflowTestSubCommand.class);
    add("hdfs", HdfsDataflowTestSubCommand.class);
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
      this.testClass = HdfsDataflowTest.class;
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
