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
import com.neverwinterdp.registry.notification.Notifier;
import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.scribengin.ScribenginCluster;
import com.neverwinterdp.swing.widget.Fonts;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;
import com.neverwinterdp.util.text.DateUtil;

@SuppressWarnings("serial")
public class UINotificationView extends SpringLayoutGridJPanel implements UILifecycle {
  private String notificationPath ;
  private NotificationJXTable notificationTable ;
  private  NotificationDetailPanel detailPanel;
  
  public UINotificationView(String logPath) {
    this.notificationPath = logPath ;
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
    Registry registry = ScribenginCluster.getCurrentInstance().getRegistry();
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
      
      notificationTable = new  NotificationJXTable(notificationPath) ;
      detailPanel = new NotificationDetailPanel() ;
      JSplitPane splitPane =
          new JSplitPane(JSplitPane.VERTICAL_SPLIT, new JScrollPane(notificationTable), new JScrollPane(detailPanel));
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
  
  public class NotificationJXTable extends JXTable {
    public NotificationJXTable(String logPath) throws Exception {
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
          int row = getSelectedRow() ;
          String seqId = model.getValueAt(row, 0).toString();
          NotificationEvent notificationEvent = model.getNotificationEvent(seqId);
          detailPanel.updateLogDetail(notificationEvent);
        }
      });
    }
  }

  static class RegistryLogTableModel extends DefaultTableModel {
    static String[] COLUMNS = {"#", "Timestamp", "Level", "Name", "Message"} ;

    private String notificationPath ;
    List<NotificationEvent> notificationEvents;
    
    public RegistryLogTableModel(String logPath) {
      super(COLUMNS, 0) ;
      this.notificationPath = logPath ;
    }
    
    public NotificationEvent getNotificationEvent(String seqId) {
      int idx = Integer.parseInt(seqId) ;
      return notificationEvents.get(idx) ;
    }
    
    void loadData() throws Exception {
      notificationEvents = loadNotificationEvents() ;
      for(int i = 0; i < notificationEvents.size(); i++) {
        NotificationEvent notificationEvent = notificationEvents.get(i) ;
        Object[] cells = {
          notificationEvent.getSeqId(),
          DateUtil.asCompactDateTime(notificationEvent.getTimestamp()), 
          notificationEvent.getLevel(),
          notificationEvent.getName(),
          notificationEvent.getMessage()
        };
        addRow(cells);
      }
    }
    
    List<NotificationEvent> loadNotificationEvents() throws Exception{
      Registry registry = ScribenginCluster.getCurrentInstance().getRegistry();
      return Notifier.getNotificationEvents(registry, notificationPath);
    }
  }
  
  public class NotificationDetailPanel extends SpringLayoutGridJPanel {
    public NotificationDetailPanel() {
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