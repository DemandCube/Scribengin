package com.neverwinterdp.vm.client.shell;

import com.neverwinterdp.vm.builder.VMClusterBuilder;
import com.neverwinterdp.vm.client.VMClient;

public class VMCommand extends Command {
  public VMCommand() {
    add("start", Start.class) ;
    add("shutdown", Shutdown.class) ;
    add("info", Info.class) ;
  }
  
  static public class Start extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      VMClient vmClient = shell.getVMClient() ;
      VMClusterBuilder clusterBuilder = new VMClusterBuilder(vmClient) ;
      clusterBuilder.start();
    }

    @Override
    public String getDescription() {
      return "start a vm";
    }
  }
  
  static public class Shutdown extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      VMClient vmClient = shell.getVMClient() ;
      vmClient.shutdown();
    }

    @Override
    public String getDescription() {
      return "shutdown a vm";
    }
  }
  
  static public class Info extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      VMClient vmClient = shell.getVMClient();
      shell.console().h1("VM Info");
      shell.console().println(VMFormater.format("Running VM", vmClient.getRunningVMDescriptors()));
      shell.console().println(VMFormater.format("History VM", vmClient.getHistoryVMDescriptors()));
    }

    @Override
    public String getDescription() {
      return "print out info about running and history vms";
    }
  }

  @Override
  public String getDescription() {
    return "Commands related to VM instances.";
  }
}