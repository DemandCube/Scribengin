package com.neverwinterdp.swing.registry;

import java.awt.BorderLayout;

import javax.swing.JLabel;
import javax.swing.JPanel;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.tool.Cluster;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;

@SuppressWarnings("serial")
public class UIActivityView extends JPanel implements UILifecycle {
  private String  activitiesRootPath ;
  private String  activityId ;
  
  public UIActivityView(String activitiesPath, String activityId) {
    setLayout(new BorderLayout()) ;
    this.activitiesRootPath = activitiesPath ;
    this.activityId = activityId ;
  }

  @Override
  public void onInit() throws Exception {
  }

  @Override
  public void onDestroy() throws Exception {
  }
  
  @Override
  public void onActivate() throws Exception {
    removeAll() ;
    Registry registry = Cluster.getCurrentInstance().getRegistry();
    if(registry == null || !registry.isConnect()) {
      add(new JLabel("No Registry Connection"), BorderLayout.CENTER);
    }
    Activity activity = 
      registry.getDataAs(activitiesRootPath + "/all/" + activityId, Activity.class);
    
    ActivityInfo activityInfo = new ActivityInfo(activity) ;
    add(activityInfo, BorderLayout.CENTER) ;
  }
  
  @Override
  public void onDeactivate() throws Exception {
    removeAll();
  }
  
  static public class ActivityInfo extends SpringLayoutGridJPanel {
    public ActivityInfo(Activity activity) {
      addRow("Id:",            activity.getId());
      addRow("Type:",          activity.getType());
      addRow("Description: ",  activity.getDescription());
      addRow("Coordinator: ",  activity.getCoordinator());
      addRow("Step Builder: ", activity.getActivityStepBuilder());
      addRow("Attributes: ",   "TODO");
      
      addRow("Logs: ",         getLogs(activity));
      makeCompactGrid();
    }
    
    private String getLogs(Activity activity) {
      StringBuilder logB = new StringBuilder() ;
      if(activity.getLogs() != null) {
        logB.append("<html>") ;
        for(String log : activity.getLogs()) {
          logB.append(log).append("<br/>");
        }
        logB.append("</html>") ;
      }
      return logB.toString();
    }
  }
}