package com.neverwinterdp.scribengin.client.shell;

import java.util.List;

import com.neverwinterdp.scribengin.client.RegistryClient;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.VMDescriptor;

public class VMResourceCommand extends Command {
  public VMResourceCommand() {
    add("list", new ListVMResourceDescriptor()) ;
  }
  
  static public class ListVMResourceDescriptor extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      RegistryClient client = shell.getRegistryClient();
      List<VMDescriptor> descriptors = client.getVMResourceDescriptors() ;
      TabularFormater formater = new TabularFormater("ID", "Path", "Cores", "Memory");
      for(int i = 0; i < descriptors.size(); i++) {
        VMDescriptor descriptor = descriptors.get(i) ;
        formater.addRow(
            descriptor.getId(), 
            descriptor.getStoredPath(),
            descriptor.getCpuCores(),
            descriptor.getMemory()
        );
      }
      formater.setTitle("Allocated VM Resources");
      shell.console().println(formater.getFormatText());
    }
  }
}
