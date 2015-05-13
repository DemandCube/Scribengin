package com.neverwinterdp.swing.registry;

import java.awt.Color;
import java.awt.event.ActionEvent;
import java.util.List;

import javax.swing.AbstractAction;
import javax.swing.JScrollPane;
import javax.swing.JToolBar;
import javax.swing.ListSelectionModel;
import javax.swing.table.DefaultTableModel;

import org.jdesktop.swingx.JXTable;
import org.jdesktop.swingx.decorator.ColorHighlighter;
import org.jdesktop.swingx.decorator.HighlightPredicate;
import org.jdesktop.swingx.decorator.HighlighterFactory;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryLogger;
import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.tool.Cluster;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;

@SuppressWarnings("serial")
public class UILogView extends SpringLayoutGridJPanel implements UILifecycle {
  private String logPath ;
  
  public UILogView(String logPath) {
    this.logPath = logPath ;
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
      
      LogJXTable logTable = new  LogJXTable(logPath) ;
      addRow(new JScrollPane(logTable)) ;
    }
    makeCompactGrid(); 
  }

  @Override
  public void onDeactivate() throws Exception {
    clear();
  }
  
  static public class LogJXTable extends JXTable {
    public LogJXTable(String logPath) throws Exception {
      setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
      RegistryLogTableModel model = new RegistryLogTableModel(logPath);
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

  static class RegistryLogTableModel extends DefaultTableModel {
    static String[] COLUMNS = {"Timestamp", "Level", "Message"} ;

    private String logPath ;
    List<RegistryLogger.Log> logs;
    
    public RegistryLogTableModel(String logPath) {
      super(COLUMNS, 0) ;
      this.logPath = logPath ;
    }
    
    void loadData() throws Exception {
      logs = loadLogs() ;
      for(int i = 0; i < logs.size(); i++) {
        RegistryLogger.Log log = logs.get(i) ;
        Object[] cells = {
          log.getTimestamp(), log.getLevel(), log.getMessage()
        };
        addRow(cells);
      }
    }
    
    List<RegistryLogger.Log> loadLogs() throws Exception{
      Registry registry = Cluster.getCurrentInstance().getRegistry();
      return registry.getChildrenAs(logPath, RegistryLogger.Log.class) ;
    }
  }
}