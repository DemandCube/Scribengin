package com.neverwinterdp.vm.client.shell;

import java.util.List;

import com.neverwinterdp.util.text.StringUtil;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;

public class VMCommand extends Command {
  public VMCommand() {
    add("list", new ListVMResourceDescriptor()) ;
  }
  
  static public class ListVMResourceDescriptor extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      VMClient vmClient = shell.getVMClient();
      List<VMDescriptor> descriptors = vmClient.getVMDescriptors() ;
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
      formater.setTitle("Allocated VM Resources");
      shell.console().println(formater.getFormatText());
    }
  }
}
