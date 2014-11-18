package com.neverwinterdp.scribengin.client.shell;

import java.util.Collections;
import java.util.List;

import com.neverwinterdp.scribengin.client.RegistryClient;
import com.neverwinterdp.scribengin.master.MasterDescriptor;
import com.neverwinterdp.util.text.TabularFormater;

public class MasterCommand extends Command {
  public MasterCommand() {
    add("list", new ListMasterDescriptor()) ;
  }
  
  static public class ListMasterDescriptor extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      RegistryClient client = shell.getRegistryClient();
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
