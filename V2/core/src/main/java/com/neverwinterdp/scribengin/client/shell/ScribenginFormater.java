package com.neverwinterdp.scribengin.client.shell;

import java.util.List;

import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.VMDescriptor;

public class ScribenginFormater {
  static public String format(String title, List<DataflowDescriptor> descriptors) {
    TabularFormater formater = new TabularFormater("Name", "App Home", "Workers", "Executor Per Worker");
    formater.setIndent("  ");
    for(int i = 0; i < descriptors.size(); i++) {
      DataflowDescriptor descriptor = descriptors.get(i) ;
      formater.addRow(
          descriptor.getName(), 
          descriptor.getDataflowAppHome(),
          descriptor.getNumberOfWorkers(),
          descriptor.getNumberOfExecutorsPerWorker()
      );
    }
    formater.setTitle(title);
    return formater.getFormatText();
  }
  
	// TODO Or should I go the long way and have DataFlowDescriptor and
	// VMDescriptor extend a Descriptor class. Thus have only 1 method here
	public static String format(String title, List<VMDescriptor> vmDescriptors, String leaderPath) {
		TabularFormater formater = new TabularFormater("name", "CPU Cores", "Memory", "Path", "is Leader");
		formater.setIndent("  ");
		for (VMDescriptor descriptor : vmDescriptors) {
			formater.addRow(descriptor.getVmConfig().getName(), 
					descriptor.getCpuCores(),
					descriptor.getMemory(), 
					descriptor.getStoredPath(), 
					descriptor.getStoredPath().equals(leaderPath));
		}
		formater.setTitle(title);
		return formater.getFormatText();
	}
}