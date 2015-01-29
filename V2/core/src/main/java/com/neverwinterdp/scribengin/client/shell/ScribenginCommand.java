package com.neverwinterdp.scribengin.client.shell;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.builder.ScribenginClusterBuilder;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.builder.VMClusterBuilder;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.shell.Command;
import com.neverwinterdp.vm.client.shell.CommandInput;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.client.shell.SubCommand;

public class ScribenginCommand extends Command {
  public ScribenginCommand() {
    add("server", new Server()) ;
    add("master", new ListMasterDescriptor()) ;
  }
  
  
  static public class Server extends SubCommand {
    @Parameter(names = "--start-master", description = "Start cluster server components")
    private String start ;
    
    @Parameter(names = "--stop-master", description = "Start cluster server components")
    private String stop ;
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      VMClient vmClient = shell.getVMClient() ;
      ScribenginClusterBuilder clusterBuilder = new ScribenginClusterBuilder(new VMClusterBuilder(vmClient)) ;
      if("vm".equals(start)) {
        clusterBuilder.startVMMasters();
      } else if("scribengin".equals(start)) {
        clusterBuilder.startScribenginMasters();
      }
      
      //TODO: for Richard
      ScribenginShell scribenginShell = (ScribenginShell) shell ;
      ScribenginClient scribenginClient = scribenginShell.getScribenginClient();
      VMDescriptor scribenginMaster = scribenginClient.getScribenginMaster() ;
      vmClient.shutdown(scribenginMaster) ;
    }
  }
  
  static public class ListMasterDescriptor extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginClient client = ((ScribenginShell)shell).getScribenginClient();
    }
  }
  
  static public class DataflowDeploy extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginClient client = ((ScribenginShell)shell).getScribenginClient();
    }
  }
}
