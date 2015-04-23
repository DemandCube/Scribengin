package com.neverwinterdp.registry.activity;

import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PreDestroy;

import com.google.inject.Injector;
import com.neverwinterdp.registry.BatchOperations;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.registry.queue.DistributedQueue;

public class ActivityService {
  static DecimalFormat     ORDER_FORMATER = new DecimalFormat("#000");

  private Injector         container;
  private Registry         registry;

  private Node             activeNode;
  private Node             historyNode;
  private DistributedQueue queue;
  private Map<String, ActivityRunner> activeActivities = new ConcurrentHashMap<String, ActivityRunner>();
  private ActivityScheduler activityScheduler ;
  
  public ActivityService() { }
  
  public ActivityService(Injector container, String activityPath) throws RegistryException {
    init(container, activityPath);
  }

  protected void init(Injector container, String activityPath) throws RegistryException {
    this.container = container;
    registry     = container.getInstance(Registry.class) ;
    activeNode  = registry.createIfNotExist(activityPath + "/active") ;
    historyNode = registry.createIfNotExist(activityPath + "/history") ;
    queue = new DistributedQueue(registry, activityPath +  "/queue");
    activityScheduler = new ActivityScheduler();
    activityScheduler.start();
  }
  
  @PreDestroy
  public void onDestroy() {
    queue.shutdown();
    activityScheduler.interrupt();
  }
  
  public <T extends ActivityCoordinator> T getActivityCoordinator(String type) throws Exception {
    return container.getInstance((Class<T>)Class.forName(type));
  }
  
  public <T extends ActivityCoordinator> T getActivityCoordinator(Class<T> type) throws Exception {
    return  container.getInstance(type);
  }
  
  public <T extends ActivityStepExecutor> T getActivityStepExecutor(String type) throws Exception {
    return container.getInstance((Class<T>)Class.forName(type));
  }
  
  public void queue(Activity activity) throws Exception {
    queue.offerAs(activity);
  }
  
  public ActivityExecutionContext run(Activity activity) throws Exception {
    create(activity);
    ActivityExecutionContext context = new ActivityExecutionContext(activity, ActivityService.this);
    ActivityRunner runner = new ActivityRunner(context, activity);
    runner.start(); 
    return context;
  }
  
  public ActivityCoordinator start(ActivityBuilder builder) throws Exception {
    Activity activity = create(builder.getActivity(), builder.getActivitySteps());
    ActivityCoordinator coordinator = getActivityCoordinator(activity.getCoordinator());
    ActivityExecutionContext context = new ActivityExecutionContext(activity, this);
    coordinator.onStart(context, activity);
    return coordinator;
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
  
  synchronized public Activity create(Activity activity) throws Exception {
    Class<ActivityStepBuilder> stepBuilderType = 
        (Class<ActivityStepBuilder>)Class.forName(activity.getActivityStepBuilder());
    ActivityStepBuilder stepBuilder = container.getInstance(stepBuilderType);
    List<ActivityStep> activitySteps = stepBuilder.build(activity, container);
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
  
  public <T> void updateActivityStepAssigned(final Activity activity, final ActivityStep step) throws RegistryException {
    BatchOperations<Boolean> ops = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        Node activityStepNode = getActivityStepNode(activity, step);
        Transaction transaction = registry.getTransaction() ;
        step.setStatus(ActivityStep.Status.ASSIGNED);
        transaction.setData(activityStepNode, step);
        transaction.commit();
        return true;
      }
    };
    registry.executeBatch(ops, 3, 1000);
  }
  
  public <T> void updateActivityStepExecuting(final Activity activity, final ActivityStep step, final T workerInfo) throws RegistryException {
    BatchOperations<Boolean> ops = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        Node activityStepNode = getActivityStepNode(activity, step);
        Transaction transaction = registry.getTransaction() ;
        step.setStatus(ActivityStep.Status.EXECUTING);
        transaction.setData(activityStepNode, step);
        transaction.createChild(activityStepNode, "heartbeat", workerInfo, NodeCreateMode.EPHEMERAL) ;
        transaction.commit();
        return true;
      }
    };
    registry.executeBatch(ops, 3, 1000);
  }
  
  public <T> void updateActivityStepFinished(final Activity activity, final ActivityStep step) throws RegistryException {
    BatchOperations<Boolean> ops = new BatchOperations<Boolean>() {
      @Override
      public Boolean execute(Registry registry) throws RegistryException {
        Node activityStepNode = getActivityStepNode(activity, step);
        Transaction transaction = registry.getTransaction() ;
        step.setStatus(ActivityStep.Status.FINISHED);
        transaction.setData(activityStepNode, step);
        transaction.deleteChild(activityStepNode, "heartbeat") ;
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
        Node activityStepNode = getActivityStepNode(activity, activityStep);
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
  
  public Node getActivityNode(Activity activity) throws RegistryException {
    return activeNode.getChild(activity.getId());
  }
  
  public Node getActivityStepNode(Activity activity, ActivityStep step) throws RegistryException {
    return activityStepNode(activity.getId(), step.getId());
  }
  
  private Node activityStepNode(String activityId, String stepId) throws RegistryException {
    return activeNode.getDescendant(activityId + "/activity-steps/" + stepId);
  }
  
  public class ActivityScheduler extends Thread {
    public void run() {
      try {
        doRun() ;
      } catch (InterruptedException e) {
      } catch (DistributedQueue.ShutdownException e) {
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    
    void doRun() throws Exception {
      Activity activity = null ;
      while((activity = queue.takeAs(Activity.class)) != null) {
        create(activity);
        ActivityExecutionContext context = new ActivityExecutionContext(activity, ActivityService.this);
        ActivityRunner runner = new ActivityRunner(context, activity);
        runner.start(); 
      }
    }
  }
  
  public class ActivityRunner extends Thread {
    private Activity activity ;
    private ActivityExecutionContext context ;
    
    public ActivityRunner(ActivityExecutionContext context, Activity activity) {
      this.context = context;
      this.activity = activity;
    }
    
    public void run() {
      try {
        activeActivities.put(activity.getId(), this);
        doRun() ;
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        activeActivities.remove(activity.getId());
      }
    }
   
    void doRun() throws Exception {
      ActivityCoordinator coordinator = getActivityCoordinator(activity.getCoordinator());
      context.setActivityCoordinator(coordinator);
      coordinator.onStart(context, activity);
    }
  }
}
