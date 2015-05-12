package com.neverwinterdp.swing.scribengin.dataflow;

import java.awt.Color;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.List;

import javax.swing.AbstractAction;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JToolBar;
import javax.swing.ListSelectionModel;
import javax.swing.table.DefaultTableModel;

import org.jdesktop.swingx.JXTable;
import org.jdesktop.swingx.decorator.ColorHighlighter;
import org.jdesktop.swingx.decorator.HighlightPredicate;
import org.jdesktop.swingx.decorator.HighlighterFactory;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowTaskExecutorDescriptor;
import com.neverwinterdp.swing.tool.Cluster;
import com.neverwinterdp.swing.util.MessageUtil;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;

@SuppressWarnings("serial")
public class UIDataflowWorkerView extends SpringLayoutGridJPanel {
  private String workersPath ;
  
  public UIDataflowWorkerView(String workersPath) {
    this.workersPath = workersPath;
    Registry registry = Cluster.getCurrentInstance().getRegistry();
    if(registry == null) {
      JPanel infoPanel = new JPanel();
      infoPanel.add(new JLabel("No Registry Connection"));
      addRow(infoPanel);
    } else {
      try {
        init(registry) ;
      } catch(Throwable e) {
        MessageUtil.handleError(e);
      }
    }
    makeCompactGrid(); 
  }
  
  
  private void init(Registry registry) throws Exception {
    JToolBar toolbar = new JToolBar();
    toolbar.setFloatable(false);
    toolbar.add(new AbstractAction("Reload") {
      @Override
      public void actionPerformed(ActionEvent e) {
      }
    });
    addRow(toolbar) ;
    
    DataflowWorkersJXTable workerTable = new  DataflowWorkersJXTable(getWorkers(registry)) ;
    addRow(new JScrollPane(workerTable)) ;
  }
  
  
  protected List<WorkerAndExecutors> getWorkers(Registry registry) throws RegistryException {
    List<WorkerAndExecutors> workersAndExecutors = new ArrayList<>();
    
    if(! registry.exists(workersPath+"/all")){
      JPanel infoPanel = new JPanel();
      infoPanel.add(new JLabel("Path: "+workersPath+"/all does not exist!"));
      addRow(infoPanel);
      return new ArrayList<WorkerAndExecutors>();
    }
    
    for(String id : registry.getChildren(workersPath+"/all")){
      workersAndExecutors.add(
          new WorkerAndExecutors(id,
              new String(registry.getData(workersPath+"/all/"+id+"/status")).replaceAll("\"", ""),
              registry.getChildrenAs(workersPath+"/all/"+id+"/executors", DataflowTaskExecutorDescriptor.class)
            ));
    }
    
    
    return workersAndExecutors;
  }
  
  static public class DataflowWorkersJXTable extends JXTable {
    public DataflowWorkersJXTable(List<WorkerAndExecutors> workersAndExecutors) throws Exception {
      setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
      DataflowWorkersTableModel model = new DataflowWorkersTableModel(workersAndExecutors);
      setModel(model);
      model.loadData();
      
      setVisibleRowCount(30);
      setVisibleColumnCount(8);
      setHorizontalScrollEnabled(true);
      setColumnControlVisible(true);

      setHighlighters(HighlighterFactory.createSimpleStriping());
      addHighlighter(new ColorHighlighter(HighlightPredicate.ROLLOVER_ROW, Color.BLACK, Color.WHITE));
    
    }
  }
  
  
  static class DataflowWorkersTableModel extends DefaultTableModel {
    static String[] COLUMNS = {"Worker ID", "Worker Status", 
      "Executor ID", "Executor Task IDs", "Executor Status"} ;

    List<WorkerAndExecutors> workersAndExecutors;
    
    public DataflowWorkersTableModel(List<WorkerAndExecutors> workersAndExecutors) {
      super(COLUMNS, 0) ;
      this.workersAndExecutors = workersAndExecutors ;
    }
    
    void loadData() throws Exception {
      for(WorkerAndExecutors wae: workersAndExecutors){
        for(DataflowTaskExecutorDescriptor executorDesc: wae.getExecutorDescriptors()){
          Object[] cell = {
              wae.getID(), 
              wae.getStatus(),
              executorDesc.getId(),
              executorDesc.getAssignedTaskIds(),
              executorDesc.getStatus()
          };
          addRow(cell);
        }
      }
    }
  }
  
  //Simple class to help map worker with executors
  public class WorkerAndExecutors{
    public String ID;
    public String status;
    public List<DataflowTaskExecutorDescriptor> executorDescs;
    
    public WorkerAndExecutors(String ID, String status, List<DataflowTaskExecutorDescriptor> executorDescs){
      this.ID = ID;
      this.status = status;
      this.executorDescs = executorDescs;
    }
    
    public String getID(){
      return this.ID;
    }
    
    public String getStatus(){
      return this.status;
    }
    
    public List<DataflowTaskExecutorDescriptor> getExecutorDescriptors(){
      return this.executorDescs;
    }
  }
  
}
