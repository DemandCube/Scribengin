package com.neverwinterdp.vm.client.shell;

import java.util.List;

import com.neverwinterdp.util.text.StringUtil;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.builder.VMClusterBuilder;
import com.neverwinterdp.vm.client.VMClient;

public class VMCommand extends Command {
  public VMCommand() {
    add("start", new Start()) ;
    add("shutdown", new Shutdown()) ;
    add("info", new Info()) ;
  }
  
  static public class Start extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      VMClient vmClient = shell.getVMClient() ;
      VMClusterBuilder clusterBuilder = new VMClusterBuilder(vmClient) ;
      clusterBuilder.start();
    }
  }
  
  static public class Shutdown extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      VMClient vmClient = shell.getVMClient() ;
      vmClient.shutdown();
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
  }
}