package com.neverwinterdp.scribengin.dataflow.activity.util;

import java.util.List;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.registry.util.NodeFormatter;
import com.neverwinterdp.util.text.TabularFormater;

public class ActivityNodeFormatter extends NodeFormatter {
  private Node activityNode;
  
  public ActivityNodeFormatter(Node activityNode){
    this.activityNode = activityNode;
  }
  
  @Override
  public String getFormattedText() {
    StringBuilder b = new StringBuilder() ;
    b.append("Activity Node: ");
    try {
      Activity activity = activityNode.getDataAs(Activity.class);
      if(activity == null) {
        b.append("Activity data is not available!");
        return b.toString();
      }
      TabularFormater activityFormater = new TabularFormater("Property", "Value");
      activityFormater.setTitle("Activity " + activity.getDescription());
      activityFormater.addRow("Id", activity.getId());
      activityFormater.addRow("Description", activity.getDescription());
      activityFormater.addRow("Type", activity.getType());
      activityFormater.addRow("Coordinator", activity.getCoordinator());
      b.append(activityFormater.getFormatText());
      
      List<ActivityStep> steps = activityNode.getChild("activity-steps").getChildrenAs(ActivityStep.class);
      TabularFormater stepFormater = new TabularFormater("Id", "Type", "Status", "Description");
      for(ActivityStep step : steps) {
        stepFormater.addRow(step.getId(), step.getType(), step.getStatus(), step.getDescription());
      }
      b.append(stepFormater.getFormatText());
    } catch (RegistryException e) {
      b.append(e.getMessage());
    }
    return b.toString();
  }
}
