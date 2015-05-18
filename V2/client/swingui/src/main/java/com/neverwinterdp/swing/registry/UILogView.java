package com.neverwinterdp.swing.registry;

import java.awt.Color;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.List;

import javax.swing.AbstractAction;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTextArea;
import javax.swing.JToolBar;
import javax.swing.ListSelectionModel;
import javax.swing.table.DefaultTableModel;

import org.jdesktop.swingx.JXTable;
import org.jdesktop.swingx.decorator.ColorHighlighter;
import org.jdesktop.swingx.decorator.HighlightPredicate;
import org.jdesktop.swingx.decorator.HighlighterFactory;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.notification.NotificationEvent;
import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.tool.Cluster;
import com.neverwinterdp.swing.widget.Fonts;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;
import com.neverwinterdp.util.text.DateUtil;

@SuppressWarnings("serial")
public class UILogView extends SpringLayoutGridJPanel implements UILifecycle {
  private String logPath ;
  private LogJXTable logTable ;
  private  LogDetailPanel logDetailPanel;
  
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
      
      logTable = new  LogJXTable(logPath) ;
      logDetailPanel = new LogDetailPanel() ;
      JSplitPane splitPane =
          new JSplitPane(JSplitPane.VERTICAL_SPLIT, new JScrollPane(logTable), new JScrollPane(logDetailPanel));
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
  
  public class LogJXTable extends JXTable {
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
      
      addMouseListener(new MouseAdapter() {
        public void mouseClicked(MouseEvent e) {
          RegistryLogTableModel model = (RegistryLogTableModel) getModel() ;
          NotificationEvent notificationEvent = model.getLogAt(getSelectedRow());
          logDetailPanel.updateLogDetail(notificationEvent);
        }
      });
    }
  }

  static class RegistryLogTableModel extends DefaultTableModel {
    static String[] COLUMNS = {"Timestamp", "Level", "Name", "Message"} ;

    private String logPath ;
    List<NotificationEvent> notificationEvents;
    
    public RegistryLogTableModel(String logPath) {
      super(COLUMNS, 0) ;
      this.logPath = logPath ;
    }
    
    public NotificationEvent getLogAt(int row) {
      return notificationEvents.get(row) ;
    }
    
    void loadData() throws Exception {
      notificationEvents = loadLogs() ;
      for(int i = 0; i < notificationEvents.size(); i++) {
        NotificationEvent notificationEvent = notificationEvents.get(i) ;
        Object[] cells = {
          DateUtil.asCompactDateTime(notificationEvent.getTimestamp()), 
          notificationEvent.getLevel(),
          notificationEvent.getName(),
          notificationEvent.getMessage()
        };
        addRow(cells);
      }
    }
    
    List<NotificationEvent> loadLogs() throws Exception{
      Registry registry = Cluster.getCurrentInstance().getRegistry();
      return registry.getChildrenAs(logPath, NotificationEvent.class) ;
    }
  }
  
  public class LogDetailPanel extends SpringLayoutGridJPanel {
    public LogDetailPanel() {
      updateLogDetail(null);
    }
    
    public void updateLogDetail(NotificationEvent notificationEvent) {
      clear() ;
      createBorder("Log Detail");
      if(notificationEvent != null) {
        addRow("Timestamp", DateUtil.asCompactDateTime(notificationEvent.getTimestamp()));
        addRow("Level",     notificationEvent.getLevel().toString());
        addRow("Name",      notificationEvent.getName());
        JTextArea text = new JTextArea() ;
        text.setFont(Fonts.FIXED);
        text.setText(notificationEvent.getMessage());
        addRow("Message", text);
        makeCompactGrid();
      }
      revalidate();
    }
  }
}