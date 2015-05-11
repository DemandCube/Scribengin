package com.neverwinterdp.swing.registry;

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
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.swing.tool.Cluster;
import com.neverwinterdp.swing.util.MessageUtil;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;

@SuppressWarnings("serial")
public class UIActivityListView extends SpringLayoutGridJPanel {
  private String activityRootPath ;
  private String listPath ;
  
  public UIActivityListView(String activityRootPath, String path) {
    this.activityRootPath = activityRootPath;
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

  public String getActivityRootPath() { return this.activityRootPath ; }
  
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
    
    ActivityJXTable activityTable = new  ActivityJXTable(getActivities(registry)) ;
    addRow(new JScrollPane(activityTable)) ;
  }
  
  protected List<Activity> getActivities(Registry registry) throws RegistryException {
    List<String> activityPaths = new ArrayList<>() ;
    for(String id : registry.getChildren(listPath)) activityPaths.add(activityRootPath + "/all/" + id) ;
    List<Activity> activities = registry.getDataAs(activityPaths, Activity.class) ;
    return activities ;
  }
  
  static public class ActivityJXTable extends JXTable {
    public ActivityJXTable(List<Activity> activities) throws Exception {
      setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
      ActivityTableModel model = new ActivityTableModel(activities);
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

  
  static class ActivityTableModel extends DefaultTableModel {
    static String[] COLUMNS = {"Id", "Coordinator", "Step Builder"} ;

    List<Activity> activities;
    
    public ActivityTableModel(List<Activity> activities) {
      super(COLUMNS, 0) ;
      this.activities = activities ;
    }
    
    void loadData() throws Exception {
      for(int i = 0; i < activities.size(); i++) {
        Activity activity = activities.get(i) ;
        Object[] cells = {
          activity.getId(), activity.getCoordinator(), activity.getActivityStepBuilder()
        };
        addRow(cells);
      }
    }
  }
}