package com.neverwinterdp.swing.scribengin.dataflow;

import java.awt.BorderLayout;
import java.util.Map;
import java.util.Map.Entry;

import javax.swing.JLabel;
import javax.swing.JPanel;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.scribengin.ScribenginCluster;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;


@SuppressWarnings("serial")
public class UIDataflowDescriptorView extends JPanel implements UILifecycle {
  private String  dataflowRootPath ;
  
  public UIDataflowDescriptorView(String dataflowRootPath) {
    
    setLayout(new BorderLayout()) ;
    this.dataflowRootPath = dataflowRootPath ;
  }
  
  @Override
  public void onInit() throws Exception {
  }

  @Override
  public void onDestroy() throws Exception {
  }

  @Override
  public void onActivate() throws Exception {
    removeAll();
    Registry registry = ScribenginCluster.getCurrentInstance().getRegistry();
    if(registry == null || !registry.isConnect()) {
      add(new JLabel("No Registry Connection"), BorderLayout.CENTER);
    }
    DataflowDescriptor dataflowDesc = 
      registry.getDataAs(dataflowRootPath, DataflowDescriptor.class);
    
    DataflowInfo dfInfo = new DataflowInfo(dataflowDesc) ;
    add(dfInfo, BorderLayout.CENTER) ;
  }

  @Override
  public void onDeactivate() throws Exception {
    removeAll();
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

      for (Entry<String, String> entry : sourceDesc.entrySet()) {
        addRow(indent+entry.getKey(), entry.getValue());
      }

      addRow("Sink Descriptors:","");
      for (Entry<String, StorageDescriptor> entry : sinkDescriptors.entrySet()) {
        String sinkName = entry.getKey();
        StorageDescriptor sinkDescriptor = entry.getValue();
        addRow(indent + sinkName + " Sink:","");
        
        //TODO: code convention , format.
        addRow(indent + "Sink Type",     sinkDescriptor.getType());
        addRow(indent + "Sink Location", sinkDescriptor.getLocation());
        for (Entry<String, String> entrySet : sinkDescriptor.entrySet()) {
          addRow(indent + entrySet.getKey(), entrySet.getValue());
        }
      }
      makeCompactGrid();
    }
  }
}
