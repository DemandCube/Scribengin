package com.neverwinterdp.swing.scribengin.dataflow;

import java.awt.BorderLayout;
import java.util.Map;
import java.util.Map.Entry;

import javax.swing.JLabel;
import javax.swing.JPanel;

import org.apache.commons.lang3.StringUtils;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.swing.tool.Cluster;
import com.neverwinterdp.swing.util.MessageUtil;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;


@SuppressWarnings("serial")
public class UIDataflowDescriptorView extends JPanel {
  private String  dataflowRootPath ;
  
  public UIDataflowDescriptorView(String dataflowRootPath) {
    
    setLayout(new BorderLayout()) ;
    this.dataflowRootPath = dataflowRootPath ;
    
    try {
      onActivate() ;
    } catch(Throwable e) {
      MessageUtil.handleError(e);
    }
  }
  
  public void onActivate() throws RegistryException {
    Registry registry = Cluster.getCurrentInstance().getRegistry();
    if(registry == null || !registry.isConnect()) {
      add(new JLabel("No Registry Connection"), BorderLayout.CENTER);
    }
    DataflowDescriptor dataflowDesc = 
      registry.getDataAs(dataflowRootPath, DataflowDescriptor.class);
    
    DataflowInfo dfInfo = new DataflowInfo(dataflowDesc) ;
    add(dfInfo, BorderLayout.CENTER) ;
  }
  
  static public class DataflowInfo extends SpringLayoutGridJPanel {
    String indent= "    ";
    public DataflowInfo(DataflowDescriptor dataflowDesc) {
      if(dataflowDesc == null){
        addRow("Dataflow Descriptor is null!","");
        return;
      }
      
      addRow("Dataflow Descriptor:","");
      addRow(indent+"Name:",                           dataflowDesc.getName());
      addRow(indent+"Id:",                             dataflowDesc.getId());
      addRow(indent+"Task Max Execute Time:",          dataflowDesc.getTaskMaxExecuteTime());
      addRow(indent+"DataflowAppHome:",                dataflowDesc.getDataflowAppHome());
      addRow(indent+"Number of Workers:",              dataflowDesc.getNumberOfWorkers());
      addRow(indent+"Number Of Executors Per Worker:", dataflowDesc.getNumberOfExecutorsPerWorker());
      addRow(indent+"Scribe:",                         dataflowDesc.getScribe());
      
      
      Map<String, StorageDescriptor> sinkDescriptors = dataflowDesc.getSinkDescriptors();
      StorageDescriptor sourceDesc = dataflowDesc.getSourceDescriptor();
      
      addRow("Source Descriptor:","");
      addRow(indent+"Source Type",sourceDesc.getType());
      addRow(indent+"Source Location", sourceDesc.getLocation());
      
      addRow("Sink Descriptors:","");
      for (Entry<String, StorageDescriptor> entry : sinkDescriptors.entrySet()) {
        String sinkName = entry.getKey();
        StorageDescriptor desc = entry.getValue();
        addRow(indent+sinkName+" Sink:","");
        addRow(StringUtils.repeat(indent, 2)+"Sink Type",     desc.getType());
        addRow(StringUtils.repeat(indent, 2)+"Sink Location", desc.getLocation());
      }
      
      
      makeCompactGrid();
    }
  }
  
}
