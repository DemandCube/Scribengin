package com.neverwinterdp.scribengin.client.shell;

import java.util.Collections;
import java.util.List;

import com.neverwinterdp.scribengin.client.ScribenginClient;
import com.neverwinterdp.scribengin.master.MasterDescriptor;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.client.shell.Command;
import com.neverwinterdp.vm.client.shell.CommandInput;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.client.shell.SubCommand;

public class ScribenginCommand extends Command {
  public ScribenginCommand() {
    add("master", new ListMasterDescriptor()) ;
  }
  
  static public class ListMasterDescriptor extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      ScribenginClient client = ((ScribenginShell)shell).getScribenginClient();
      List<MasterDescriptor> descriptors = client.getScribenginMasterDescriptors() ;
      Collections.sort(descriptors, MasterDescriptor.COMPARATOR);
      TabularFormater formater = new TabularFormater("ID", "Type");
      for(int i = 0; i < descriptors.size(); i++) {
        MasterDescriptor descriptor = descriptors.get(i) ;
        formater.addRow(
            descriptor.getId(), descriptor.getType()
        );
      }
      formater.setTitle("Available Masters");
      shell.console().println(formater.getFormatText());
    }
  }
}
