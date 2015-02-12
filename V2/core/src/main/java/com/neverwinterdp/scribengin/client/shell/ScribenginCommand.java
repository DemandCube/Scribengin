package com.neverwinterdp.scribengin.client.shell;

import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.vm.builder.VMClusterBuilder;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.shell.Command;
import com.neverwinterdp.vm.client.shell.CommandInput;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.client.shell.SubCommand;

public class ScribenginCommand extends Command {
  public ScribenginCommand() {
    add("start",  Start.class) ;
    add("shutdown",  Shutdown.class) ;
    add("info", Info.class) ;
    //Anthony TODO: add master command and list the scribengin masters in the table format
  }
  
  static public class Start extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      VMClient vmClient = shell.getVMClient() ;
      ScribenginClusterBuilder clusterBuilder = new ScribenginClusterBuilder(new VMClusterBuilder(vmClient)) ;
      clusterBuilder.startScribenginMasters();
    }
  }
  
  static public class Shutdown extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginClient client = ((ScribenginShell)shell).getScribenginClient();
      client.shutdown();
    }
  }
  
  static public class Info extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginClient client = ((ScribenginShell)shell).getScribenginClient();
      shell.console().h1("Scribengin Info");
      shell.console().println(ScribenginFormater.format("Running Dataflows", client.getRunningDataflowDescriptor()));
      shell.console().println(ScribenginFormater.format("History Dataflows", client.getHistoryDataflowDescriptor()));
    }
  }
}
