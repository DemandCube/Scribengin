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
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskReport;
import com.neverwinterdp.swing.tool.Cluster;
import com.neverwinterdp.swing.util.MessageUtil;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;


@SuppressWarnings("serial")
public class UIDataflowTaskView extends SpringLayoutGridJPanel {
  private String tasksPath;
  
  public UIDataflowTaskView(String tasksPath) {
    this.tasksPath = tasksPath;
    
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
    
    DataflowTasksJXTable dataflowTaskTable = new  DataflowTasksJXTable(getTasks(registry)) ;
    addRow(new JScrollPane(dataflowTaskTable)) ;
  }
  
  protected List<TaskAndReport> getTasks(Registry registry) throws RegistryException {
    List<TaskAndReport> tasksAndReports = new ArrayList<>();
    if(! registry.exists(tasksPath+"/descriptors")){
      JPanel infoPanel = new JPanel();
      infoPanel.add(new JLabel("Path: "+tasksPath+"/descriptors does not exist!"));
      addRow(infoPanel);
      return new ArrayList<TaskAndReport>();
    }
    for(String id : registry.getChildren(tasksPath+"/descriptors")){
      tasksAndReports.add(
          new TaskAndReport(id,
              registry.getDataAs(tasksPath+"/descriptors/" + id, DataflowTaskDescriptor.class),
              registry.getDataAs(tasksPath+"/descriptors/" + id+"/report", DataflowTaskReport.class) 
            ));
    }
    
    
    return tasksAndReports;
  }
  
  
  static public class DataflowTasksJXTable extends JXTable {
    public DataflowTasksJXTable(List<TaskAndReport> tasksAndReports) throws Exception {
      setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
      DataflowTaskTableModel model = new DataflowTaskTableModel(tasksAndReports);
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
  
  static class DataflowTaskTableModel extends DefaultTableModel {
    static String[] COLUMNS = {"Id", "Status", "Process Count", 
                              "Commit Process Count", "Start Time", 
                              "Finish Time"} ;

    List<TaskAndReport> tasksAndReports;
    
    public DataflowTaskTableModel(List<TaskAndReport> tasksAndReports) {
      super(COLUMNS, 0) ;
      this.tasksAndReports = tasksAndReports ;
    }
    
    void loadData() throws Exception {
      for(TaskAndReport tar: tasksAndReports){
        DataflowTaskReport report = tar.getReport();
        DataflowTaskDescriptor desc = tar.getTaskDescriptor();
        
        Object[] cells = {
          tar.getID(), desc.getStatus(), report.getProcessCount(),
          report.getCommitProcessCount(), report.getStartTime(), report.getFinishTime()
        };
        addRow(cells);
      }
    }
  }
  
  //Simple class to help map taskDescriptor with its Report and ID
  public class TaskAndReport{
    public String ID;
    public DataflowTaskDescriptor dataflowTaskDesc;
    public DataflowTaskReport report;
    public TaskAndReport(String ID, DataflowTaskDescriptor dataflowTaskDesc, DataflowTaskReport report){
      this.ID = ID;
      this.dataflowTaskDesc = dataflowTaskDesc;
      this.report = report;
    }
    
    public String getID(){
      return ID;
    }
    
    public DataflowTaskReport getReport(){
      return report;
    }
    
    public DataflowTaskDescriptor getTaskDescriptor(){
      return dataflowTaskDesc;
    }
  }
  
  
}
