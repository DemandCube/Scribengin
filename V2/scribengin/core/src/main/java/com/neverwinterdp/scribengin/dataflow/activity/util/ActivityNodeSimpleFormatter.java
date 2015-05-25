package com.neverwinterdp.scribengin.dataflow.activity.util;

import java.util.List;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.registry.util.NodeFormatter;

public class ActivityNodeSimpleFormatter extends NodeFormatter {
  private Node activityNode;
  
  public ActivityNodeSimpleFormatter(Node activityNode){
    this.activityNode = activityNode;
  }
  
  @Override
  public String getFormattedText() {
    StringBuilder b = new StringBuilder() ;
    try {
      Activity activity = activityNode.getDataAs(Activity.class);
      if(activity == null) {
        return "  Activity: Activity data is not available!";
      }
      
      b.append("  Activity: ").
        append("ID = " + activity.getId() + ", ").
        append("Type = " + activity.getType() + ", ").
        append("Description = " + activity.getDescription()+"\n");
      
      b.append("  Activity Steps: ");
      List<ActivityStep> steps = activityNode.getChild("activity-steps").getChildrenAs(ActivityStep.class);
      for(ActivityStep step : steps) {
        b.append("step:" + step.getId() + " = " + step.getStatus() + ", ");
      }
    } catch (RegistryException e) {
      b.append(e.getMessage());
    }
    b.append("\n");
    return b.toString();
  }
}
