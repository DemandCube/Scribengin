package com.neverwinterdp.registry.activity;

import java.text.DecimalFormat;
import java.util.List;

import com.neverwinterdp.registry.BatchOperations;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;

public class ActivityService {
  static DecimalFormat ORDER_FORMATER = new DecimalFormat("#000");
  
  private Registry registry;
  private Node     activeNode;
  private Node     historyNode;
  
  public ActivityService(Registry registry, String activityPath) throws RegistryException {
    this.registry     = registry ;
    activeNode  = registry.createIfNotExist(activityPath + "/active") ;
    historyNode = registry.createIfNotExist(activityPath + "/history") ;
  }
  
  public Activity create(ActivityBuilder builder) throws RegistryException {
    return create(builder.getActivity(), builder.getActivitySteps());
  }
  
  public Activity create(Activity activity, List<ActivityStep> activitySteps) throws RegistryException {
    Node activityNode = activeNode.createChild(activity.getType() + "-", NodeCreateMode.PERSISTENT_SEQUENTIAL);
    activity.setId(activityNode.getName());
    Transaction transaction = registry.getTransaction() ;
    transaction.setData(activityNode, activity);
    transaction.createChild(activityNode, "activity-steps", NodeCreateMode.PERSISTENT);
    for(int i = 0; i < activitySteps.size(); i++) {
      ActivityStep step = activitySteps.get(i) ;
      String id = ORDER_FORMATER.format(i) + "-" + step.getType();
      step.setId(id);
      transaction.createDescendant(activityNode, "activity-steps/" + id, step, NodeCreateMode.PERSISTENT);
    }
    transaction.commit();
    return activity;
  }
  
  public Activity getActivity(String name) throws RegistryException {
    return activeNode.getChild(name).getDataAs(Activity.class) ;
  }
  
  public List<Activity> getActiveActivities() throws RegistryException {
    return activeNode.getChildrenAs(Activity.class) ;
  }
  
  public List<Activity> getHistoryActivities() throws RegistryException {
    return historyNode.getChildrenAs(Activity.class) ;
  }
  
  public List<ActivityStep> getActivitySteps(Activity activity) throws RegistryException {
    return getActivitySteps(activity.getId()) ;
  }
  
  public ActivityStep getActivityStep(String activityName, String stepName) throws RegistryException {
    Node stepNode = activityStepNode(activityName, stepName);
    return stepNode.getDataAs(ActivityStep.class) ;
  }
  
  public List<ActivityStep> getActivitySteps(String name) throws RegistryException {
    Node stepsNode = activeNode.getDescendant(name + "/activity-steps");
    return stepsNode.getChildrenAs(ActivityStep.class) ;
  }
  
  public <T> void assign(final Activity activity, final ActivityStep activityStep, final T workerInfo) throws RegistryException {
    BatchOperations<Boolean> ops = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        Node activityStepNode = activityStepNode(activity, activityStep);
        Transaction transaction = registry.getTransaction() ;
        activityStep.setStatus(ActivityStep.Status.ASSIGNED);
        transaction.setData(activityStepNode, activityStep);
        transaction.createChild(activityStepNode, "heartbeat", workerInfo, NodeCreateMode.EPHEMERAL) ;
        transaction.commit();
        return true;
      }
    };
    registry.executeBatch(ops, 3, 1000);
  }
  
  public void finish(final Activity activity, final ActivityStep activityStep) throws RegistryException {
    BatchOperations<Boolean> ops = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        Node activityStepNode = activityStepNode(activity, activityStep);
        Transaction transaction = registry.getTransaction() ;
        activityStep.setStatus(ActivityStep.Status.FINISHED);
        transaction.setData(activityStepNode, activityStep);
        transaction.deleteChild(activityStepNode, "heartbeat") ;
        transaction.commit();
        return true;
      }
    };
    registry.executeBatch(ops, 3, 1000);
  }
  
  public void history(Activity activity) throws RegistryException {
    String fromPath = activeNode.getChild(activity.getId()).getPath() ;
    String toPath   = historyNode.getChild(activity.getId()).getPath() ;
    Transaction transaction = registry.getTransaction();
    transaction.rcopy(fromPath, toPath);
    transaction.rdelete(fromPath);
    transaction.commit();
  }
  
  private Node activityNode(Activity activity) throws RegistryException {
    return activeNode.getChild(activity.getId());
  }
  
  private Node activityStepsNode(Activity activity) throws RegistryException {
    return activeNode.getChild(activity.getId() + "/activity-steps");
  }
  
  private Node activityStepNode(Activity activity, ActivityStep step) throws RegistryException {
    return activityStepNode(activity.getId(), step.getId());
  }
  
  private Node activityStepNode(String activityId, String stepId) throws RegistryException {
    return activeNode.getDescendant(activityId + "/activity-steps/" + stepId);
  }
}
