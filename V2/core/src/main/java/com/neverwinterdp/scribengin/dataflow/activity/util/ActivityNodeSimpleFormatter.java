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
        return "Activity Node: Activity data is not available!";
      }
      
      StringBuilder stepsString = new StringBuilder();
      List<ActivityStep> steps = activityNode.getChild("activity-steps").getChildrenAs(ActivityStep.class);
      for(ActivityStep step : steps) {
        stepsString.append("step:"+step.getId()+" = "+step.getStatus()+", ");
      }
      b.append("  Activity: ID = "+activity.getId()+", ");
      b.append("Type = "+activity.getType()+", ");
      b.append("Description = "+activity.getDescription()+"\n");
      b.append("  Activity Steps: ");
      b.append(stepsString.toString().substring(0, stepsString.toString().length()-2));
    } catch (RegistryException e) {
      b.append(e.getMessage());
    }
    b.append("\n");
    return b.toString();
  }

}
