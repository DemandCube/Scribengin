package com.neverwinterdp.scribengin.client.shell;

import java.util.List;

import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.util.text.TabularFormater;

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
}