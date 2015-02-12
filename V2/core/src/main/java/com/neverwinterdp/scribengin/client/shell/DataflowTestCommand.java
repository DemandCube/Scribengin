package com.neverwinterdp.scribengin.client.shell;

import com.neverwinterdp.scribengin.dataflow.test.DataflowTest;
import com.neverwinterdp.scribengin.dataflow.test.KafkaDataflowTest;
import com.neverwinterdp.vm.client.shell.Command;
import com.neverwinterdp.vm.client.shell.CommandInput;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.client.shell.SubCommand;

public class DataflowTestCommand extends Command {
  
  public DataflowTestCommand() {
    add("kafka", DataflowTestSubCommand.class);
  }

  static public class DataflowTestSubCommand<T extends DataflowTest> extends SubCommand {
    Class<? extends DataflowTest> testClass ;
    
    public DataflowTestSubCommand() {
      this.testClass = KafkaDataflowTest.class;
    }
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginShell scribenginShell = (ScribenginShell) shell;
      DataflowTest test = testClass.newInstance() ;
      cmdInput.mapRemainArgs(test);
      test.run(scribenginShell);
    }
  }
}