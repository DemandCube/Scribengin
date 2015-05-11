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
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.service.ScribenginService;
import com.neverwinterdp.swing.tool.Cluster;
import com.neverwinterdp.swing.util.MessageUtil;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;

@SuppressWarnings("serial")
public class UIDataflowListView extends SpringLayoutGridJPanel {
  private String listPath ;
  
  public UIDataflowListView(String path) {
    this.listPath = path ;
    Registry registry = Cluster.getCurrentInstance().getRegistry();
    if(registry == null) {
      initNoConnection() ;
    } else {
      try {
        init(registry) ;
      } catch(Throwable e) {
        MessageUtil.handleError(e);
      }
    }
    makeCompactGrid(); 
  }

  public String getListPath() { return this.listPath; }
  
  private void initNoConnection() {
    JPanel infoPanel = new JPanel();
    infoPanel.add(new JLabel("No Registry Connection"));
    addRow(infoPanel);
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
    
    DataflowJXTable dataflowTable = new  DataflowJXTable(getDescriptors(registry)) ;
    addRow(new JScrollPane(dataflowTable)) ;
  }
  
  protected List<DataflowDescriptor> getDescriptors(Registry registry) throws RegistryException {
    List<String> dataflowPaths = new ArrayList<>() ;
    for(String id : registry.getChildren(listPath)) dataflowPaths.add(ScribenginService.DATAFLOWS_ALL_PATH + "/" + id) ;
    List<DataflowDescriptor> dataflows = registry.getDataAs(dataflowPaths, DataflowDescriptor.class) ;
    return dataflows ;
  }
  
  static public class DataflowJXTable extends JXTable {
    public DataflowJXTable(List<DataflowDescriptor> activities) throws Exception {
      setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
      DataflowTableModel model = new DataflowTableModel(activities);
      setModel(model);
      model.loadData();
      
      setVisibleRowCount(30);
      setVisibleColumnCount(8);
      setHorizontalScrollEnabled(true);
      setColumnControlVisible(true);

      setHighlighters(HighlighterFactory.createSimpleStriping());
      // ...oops! we forgot one
      addHighlighter(new ColorHighlighter(HighlightPredicate.ROLLOVER_ROW, Color.BLACK, Color.WHITE));
    
    }
  }

  
  static class DataflowTableModel extends DefaultTableModel {
    static String[] COLUMNS = {"Id", "Name", "Workers", "Executors Per Worker", "Scribe"} ;

    List<DataflowDescriptor> dataflows;
    
    public DataflowTableModel(List<DataflowDescriptor> dataflows) {
      super(COLUMNS, 0) ;
      this.dataflows = dataflows ;
    }
    
    void loadData() throws Exception {
      for(int i = 0; i < dataflows.size(); i++) {
        DataflowDescriptor dataflow = dataflows.get(i) ;
        Object[] cells = {
          dataflow.getId(), dataflow.getName(), 
          dataflow.getNumberOfWorkers(), dataflow.getNumberOfExecutorsPerWorker(),
          dataflow.getScribe()
        };
        addRow(cells);
      }
    }
  }
}