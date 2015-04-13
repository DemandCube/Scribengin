package com.neverwinterdp.scribengin.dataflow.activity.util;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.registry.activity.ActivityStepResult;
import com.neverwinterdp.registry.util.NodeFormatter;
import com.neverwinterdp.util.text.TabularFormater;

public class ActiveActivityRegistryFormatter extends NodeFormatter {
  Node node;
  
  public ActiveActivityRegistryFormatter(Node n){
    this.node = n;
  }
  
  @Override
  public String getFormattedText() {
    TabularFormater t = null;
    try {
      if(!this.node.exists()){
        return "Activity nodes do not exist!";
      }
      t = new TabularFormater("Activity Name","StepID", "Status", "Result", "Description");
      
      for(String activityName: this.node.getChildren()){
        Activity activity = this.node.getChild(activityName).getDataAs(Activity.class);
        if(activity == null){
          return "Activity is not initialized!";
        }
        
        t.addRow(this.node.getChild(activityName).getName(), "", "", "", activity.getDescription());
        
        if(this.node.getChild(activityName).hasChild("activity-steps")){
          for(String child: this.node.getChild(activityName).getChild("activity-steps").getChildren()){
            ActivityStep step = this.node.getChild(activityName).getChild("activity-steps")
                                    .getChild(child).getDataAs(ActivityStep.class);
            if(step != null){
              ActivityStepResult res = step.getResult();
              
              String resultData = "";
              if(res != null){
                resultData = "Data: ".concat(new String(res.getData()));
                resultData = resultData.concat("\nError: "+step.getResult().getError());
              }
              t.addRow("", step.getId(), step.getStatus(),
                   resultData, step.getDescription());
            }
          }
        }
      }
    } catch (RegistryException e) {
      e.printStackTrace();
      return e.getMessage();
    }
    
    return t.getFormatText();
  }

}
