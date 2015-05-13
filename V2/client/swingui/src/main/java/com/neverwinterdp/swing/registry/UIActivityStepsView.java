package com.neverwinterdp.swing.registry;

import java.awt.BorderLayout;
import java.util.List;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import org.jdesktop.swingx.JXTaskPane;
import org.jdesktop.swingx.JXTaskPaneContainer;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.tool.Cluster;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;

@SuppressWarnings("serial")
public class UIActivityStepsView extends JPanel implements UILifecycle {
  private String  activitiesRootPath ;
  private String  activityId ;
  
  public UIActivityStepsView(String activitiesPath, String activityId) {
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
    List<ActivityStep> activitySteps = 
      registry.getChildrenAs(activitiesRootPath + "/all/" + activityId + "/activity-steps", ActivityStep.class);
    
    JXTaskPaneContainer tpc = new JXTaskPaneContainer();
    
    for(ActivityStep step : activitySteps) {
      JXTaskPane stepPane = new JXTaskPane(step.getId());
      stepPane.setName(step.getId());
      stepPane.add(new ActivityStepInfo(step));
      tpc.add(stepPane);
    }
    
    add(new JScrollPane(tpc), BorderLayout.CENTER) ;
  }
  
  @Override
  public void onDeactivate() throws Exception {
    removeAll() ;
  }
  
  static public class ActivityStepInfo extends SpringLayoutGridJPanel {
    public ActivityStepInfo(ActivityStep step) {
      addRow("Id:",            step.getId());
      addRow("Type:",          step.getType());
      addRow("Description: ",  step.getDescription());
      addRow("Status: ",       step.getStatus().toString());
      addRow("Max Retries: ",  step.getMaxRetries());
      addRow("Attributes: ",   "TODO");
      
      addRow("Try Count: ",    step.getTryCount());
      addRow("Execute Time: ", step.getExecuteTime() + "ms");
      addRow("Logs: ",         getLogs(step));
      makeCompactGrid();
    }
    
    private String getLogs(ActivityStep step) {
      StringBuilder logB = new StringBuilder() ;
      if(step.getLogs() != null) {
        logB.append("<html>") ;
        for(String log : step.getLogs()) {
          logB.append(log).append("<br/>");
        }
        logB.append("</html>") ;
      }
      return logB.toString();
    }
  }
}