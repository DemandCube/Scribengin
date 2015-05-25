package com.neverwinterdp.vm.client.shell;

import java.util.Map;

import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.tool.VMClusterBuilder;
import com.neverwinterdp.vm.util.VMRegistryFormatter;

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
      return "This will start the first vm, vm-master. The vm-master will be responsible for managing, starting, and stopping other VMs upon request";
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
      return "This command will shutdown all the running vm, and then shutdown the vm-master. This is a dangerous operation - the cluster will not shutdown properly unless you have already shutdown all running VMs properly";
    }
  }
  
  static public class Info extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      VMClient vmClient = shell.getVMClient();
      shell.console().h1("VM Info");
      VMRegistryFormatter regFormatter = new VMRegistryFormatter(null);
      
      TabularFormater tabFormatter = new TabularFormater("Name", "Hostname", "Description", "Memory",
                                                         "CPU Cores", "Stored Path", "Roles");
      
      
      shell.console().println("Running VM:");
      for(VMDescriptor desc : vmClient.getActiveVMDescriptors()){
        Map<String,String> data = regFormatter.getFormattedMap(desc);
        tabFormatter.addRow(data.get("Name"), data.get("Hostname"),
                            data.get("Description"), data.get("Memory"),
                            data.get("CPU Cores"), data.get("Stored Path"),
                            data.get("Roles"));
      }
      shell.console().println(tabFormatter.getFormatText());
      
      tabFormatter = new TabularFormater("Name", "Hostname", "Description", "Memory",
          "CPU Cores", "Stored Path", "Roles");
      shell.console().println("History VM:");
      for(VMDescriptor desc : vmClient.getHistoryVMDescriptors()){
        Map<String,String> data = regFormatter.getFormattedMap(desc);
        tabFormatter.addRow(data.get("Name"), data.get("Hostname"),
                            data.get("Description"), data.get("Memory"),
                            data.get("CPU Cores"), data.get("Stored Path"),
                            data.get("Roles"));
      }
      shell.console().println(tabFormatter.getFormatText());
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