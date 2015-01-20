package com.neverwinterdp.vm.client.shell;

import java.io.IOException;
import java.util.List;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.util.text.StringUtil;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;

public class VMCommand extends Command {
  public VMCommand() {
    add("list", new ListRunning()) ;
    add("history", new ListHistory()) ;
  }
  
  static public class ListRunning extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws IOException  {
      VMClient vmClient = shell.getVMClient();
      try {
        printVMDescriptors("Running VM", shell, vmClient.getRunningVMDescriptors());
      } catch (RegistryException e) {
        shell.console().println("Error caught: "+e.getMessage());
      } catch (Exception e) {
        shell.console().println("Error caught: "+e.getMessage());
      }
    }
  }
  
  static public class ListHistory extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      VMClient vmClient = shell.getVMClient();
      printVMDescriptors("History VM", shell, vmClient.getHistoryVMDescriptors());
    }
  }
  
  static void printVMDescriptors(String title, Shell shell, List<VMDescriptor> descriptors) throws Exception {
    TabularFormater formater = new TabularFormater("ID", "Path", "Roles", "Cores", "Memory");
    for(int i = 0; i < descriptors.size(); i++) {
      VMDescriptor descriptor = descriptors.get(i) ;
      formater.addRow(
          descriptor.getId(), 
          descriptor.getStoredPath(),
          StringUtil.join(descriptor.getVmConfig().getRoles(), ","),
          descriptor.getCpuCores(),
          descriptor.getMemory()
      );
    }
    formater.setTitle(title);
    shell.console().println(formater.getFormatText());
  }
}
