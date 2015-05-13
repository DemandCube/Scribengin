package com.neverwinterdp.swing.scribengin.dataflow;

import java.awt.Color;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.List;

import javax.swing.AbstractAction;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JToolBar;
import javax.swing.ListSelectionModel;
import javax.swing.table.DefaultTableModel;

import org.jdesktop.swingx.JXTable;
import org.jdesktop.swingx.decorator.ColorHighlighter;
import org.jdesktop.swingx.decorator.HighlightPredicate;
import org.jdesktop.swingx.decorator.HighlighterFactory;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.tool.Cluster;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;

@SuppressWarnings("serial")
public class UIDataflowWorkerView extends SpringLayoutGridJPanel implements UILifecycle {
  private String workersPath ;
  private DataflowWorkersJXTable  workerTable;
  private DataflowWorkerInfoPanel workerInfo ;
  
  public UIDataflowWorkerView(String workersPath) {
    this.workersPath = workersPath;
  }
  
  @Override
  public void onInit() throws Exception {
  }


  @Override
  public void onDestroy() throws Exception {
  }


  @Override
  public void onActivate() throws Exception {
    clear();
    Registry registry = Cluster.getCurrentInstance().getRegistry();
    if(registry == null) {
      addRow("No Registry Connection");
    } else {
      JToolBar toolbar = new JToolBar();
      toolbar.setFloatable(false);
      toolbar.add(new AbstractAction("Reload") {
        @Override
        public void actionPerformed(ActionEvent e) {
        }
      });
      addRow(toolbar) ;
      
      workerTable = new  DataflowWorkersJXTable(workersPath + "/all", workersPath + "/all") ;
      workerInfo = new DataflowWorkerInfoPanel() ;
      JSplitPane splitPane =
          new JSplitPane(JSplitPane.VERTICAL_SPLIT, new JScrollPane(workerTable), new JScrollPane(workerInfo));
      splitPane.setOneTouchExpandable(true);
      splitPane.setDividerLocation(150);

      addRow(splitPane) ;
    }
    makeCompactGrid(); 
  }


  @Override
  public void onDeactivate() throws Exception {
    clear();    
  }
  
  public class DataflowWorkersJXTable extends JXTable {
    public DataflowWorkersJXTable(String workerAllPath, String workerListPath) throws Exception {
      setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
      DataflowWorkersTableModel model = new DataflowWorkersTableModel(workerAllPath, workerListPath);
      setModel(model);
      model.loadData();
      
      setVisibleRowCount(30);
      setVisibleColumnCount(8);
      setHorizontalScrollEnabled(true);
      setColumnControlVisible(true);
      addMouseListener(new MouseAdapter() {
        public void mouseClicked(MouseEvent e) {
          DataflowWorkersTableModel model = (DataflowWorkersTableModel) getModel() ;
          DataflowWorkerInfo selectedWorkerInfo = model.getDataflowWorkerInfoAt(getSelectedRow());
          workerInfo.updateWorkerInfo(selectedWorkerInfo);
        }
      });
      setHighlighters(HighlighterFactory.createSimpleStriping());
      addHighlighter(new ColorHighlighter(HighlightPredicate.ROLLOVER_ROW, Color.BLACK, Color.WHITE));
    }
  }
  
  public class DataflowWorkerInfoPanel extends SpringLayoutGridJPanel {
    public DataflowWorkerInfoPanel() {
      updateWorkerInfo(null);
    }
    
    public void updateWorkerInfo(DataflowWorkerInfo selectedWorkerInfo) {
      clear() ;
      createBorder("Dataflow Worker Info");
      if(selectedWorkerInfo == null) {
        addRow("Name 1", "Value 1");
        addRow("Name 2", "Value 2");
        addRow("Name 3", "Value 3");
      } else {
        Registry registry = Cluster.getCurrentInstance().getRegistry() ;
        addRow("TODO", "Load the data from " + selectedWorkerInfo.getWorkerId());
        //TODO: load the dataflow worker descriptor, show it in the 
        //TODO load the data from selectedWorkerInfo.getDataflowWorkerPath();
      }
      makeCompactGrid();
      revalidate();
    }
  }
  
  static class DataflowWorkersTableModel extends DefaultTableModel {
    static String[] COLUMNS = {
      "Worker ID", "Worker Status", 
    } ;

    private String workerAllPath ;
    private String workerListPath ;
    private List<DataflowWorkerInfo> workerInfos ;
    
    public DataflowWorkersTableModel(String workerAllPath, String workerListPath) {
      super(COLUMNS, 0) ;
      this.workerAllPath = workerAllPath;
      this.workerListPath = workerListPath;
    }
    
    public DataflowWorkerInfo getDataflowWorkerInfoAt(int row) {
      return workerInfos.get(row) ;
    }
    
    void loadData() throws Exception {
      workerInfos = loadWorkerInfos() ;
      for(DataflowWorkerInfo workerInfo: workerInfos){
        Object[] cell = {
            workerInfo.getWorkerId(), 
            workerInfo.getStatus(),
        };
        addRow(cell);
      }
    }
    
    List<DataflowWorkerInfo> loadWorkerInfos() throws Exception {
      Registry registry = Cluster.getCurrentInstance().getRegistry();
      List<DataflowWorkerInfo> workerInfos = new ArrayList<>();
      for(String workerId : registry.getChildren(workerListPath)) {
        String status = new String(registry.getData(workerAllPath + "/" + workerId + "/status")) ;
        workerInfos.add(new DataflowWorkerInfo(workerAllPath + "/" + workerId, workerId, status));
      }
      return workerInfos;
    }
  }
  
  static public class DataflowWorkerInfo {
    private String dataflowWorkerPath ;
    public String  workerId;
    public String  status;

    public DataflowWorkerInfo(String dataflowWorkerPath, String workerId, String status){
      this.dataflowWorkerPath = dataflowWorkerPath;
      this.workerId = workerId;
      this.status = status;
    }
    
    public String getDataflowWorkerPath() { return this.dataflowWorkerPath ; }
    
    public String getWorkerId() { return this.workerId; }
    
    public String getStatus() { return this.status; }
  }
}