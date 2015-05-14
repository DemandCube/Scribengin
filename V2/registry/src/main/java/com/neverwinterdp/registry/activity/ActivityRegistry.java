package com.neverwinterdp.registry.activity;

import java.util.Collections;
import java.util.List;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;

public class ActivityRegistry {
  protected Registry                    registry;
  protected Node                        allNode;
  protected Node                        activeNode;
  protected Node                        historyNode;
  
  public ActivityRegistry() {
  }
  
  public ActivityRegistry(Registry registry, String activityPath) throws RegistryException {
    init(registry, activityPath, false);
  }

  protected void init(Registry registry, String activityPath, boolean createStructure) throws RegistryException {
    this.registry    = registry ;
    if(createStructure) {
      allNode     = registry.createIfNotExist(activityPath + "/all") ;
      activeNode  = registry.createIfNotExist(activityPath + "/active") ;
      historyNode = registry.createIfNotExist(activityPath + "/history") ;
    } else {
      allNode     = registry.get(activityPath + "/all") ;
      activeNode  = registry.get(activityPath + "/active") ;
      historyNode = registry.get(activityPath + "/history") ;
    }
  }
  
  public Activity getActivity(String name) throws RegistryException {
    return allNode.getChild(name).getDataAs(Activity.class) ;
  }
  
  public List<Activity> getActivities() throws RegistryException {
    return allNode.getChildrenAs(Activity.class) ;
  }
  
  public List<Activity> getActiveActivities() throws RegistryException {
    List<String> children = activeNode.getChildren() ;
    return allNode.getSelectChildrenAs(children, Activity.class);
  }
  
  
  public List<Activity> getHistoryActivities() throws RegistryException {
    List<String> children = historyNode.getChildren() ;
    return allNode.getSelectChildrenAs(children, Activity.class);
  }
  
  public List<ActivityStep> getActivitySteps(Activity activity) throws RegistryException {
    return getActivitySteps(activity.getId()) ;
  }
  
  public ActivityStep getActivityStep(String activityName, String stepName) throws RegistryException {
    Node stepNode = activityStepNode(activityName, stepName);
    return stepNode.getDataAs(ActivityStep.class) ;
  }
  
  public List<ActivityStep> getActivitySteps(String name) throws RegistryException {
    Node stepsNode = allNode.getDescendant(name + "/activity-steps");
    List<ActivityStep> steps = stepsNode.getChildrenAs(ActivityStep.class) ;
    Collections.sort(steps, ActivityStep.COMPARATOR);
    return steps ;
  }
  
  public void history(Activity activity) throws RegistryException {
    Transaction transaction = registry.getTransaction();
    transaction.createChild(historyNode, activity.getId(), NodeCreateMode.PERSISTENT);
    transaction.deleteChild(activeNode, activity.getId());
    transaction.commit();
  }
  
  public Node getActivityNode(Activity activity) throws RegistryException {
    return allNode.getChild(activity.getId());
  }
  
  public Node getActivityStepNode(Activity activity, ActivityStep step) throws RegistryException {
    return activityStepNode(activity.getId(), step.getId());
  }
  
  private Node activityStepNode(String activityId, String stepId) throws RegistryException {
    return allNode.getDescendant(activityId + "/activity-steps/" + stepId);
  }
}