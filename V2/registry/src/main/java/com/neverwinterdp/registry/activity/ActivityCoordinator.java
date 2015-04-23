package com.neverwinterdp.registry.activity;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Inject;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeWatcher;
import com.neverwinterdp.registry.event.RegistryListener;

abstract public class ActivityCoordinator {
  private Registry registry ;
  private RegistryListener registryListener ;
  
  
  @Inject
  public void init(Registry registry) {
    this.registry = registry ;
    registryListener = new RegistryListener(registry) ;
  }
  
  public void onStart(ActivityExecutionContext ctx, Activity activity) throws RegistryException {
    ActivityService service = ctx.getActivityService();
    List<ActivityStep> nextSteps = findNextActivitySteps(service, activity);
    for(int i = 0; i < nextSteps.size(); i++) {
      ActivityStep nextStep = nextSteps.get(i);
      nextStep.setStatus(ActivityStep.Status.ASSIGNED);
      schedule(ctx, activity, nextStep);
    }
  }
  
  public void onResume(ActivityExecutionContext ctx, Activity activity) {
    
  }
  
  public void onExecuting(ActivityExecutionContext ctx, Activity activity, ActivityStep step) {
  }
  
  public void onBroken(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws RegistryException {
    schedule(ctx, activity, step);
  }
  
  
  public void onFinish(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws RegistryException {
    ActivityService service = ctx.getActivityService();
    List<ActivityStep> nextSteps = findNextActivitySteps(service, activity);
    if(nextSteps.size() > 0) {
      for(int i = 0; i < nextSteps.size(); i++) {
        ActivityStep nextStep = nextSteps.get(i);
        nextStep.setStatus(ActivityStep.Status.ASSIGNED);
        schedule(ctx, activity, nextStep);
      } 
    } else {
      onFinish(service, activity);
    }
  }
  
  public void onFinish(ActivityService service, Activity activity) throws RegistryException {
    service.history(activity);
    synchronized(this) {
      notifyAll();
    }
    //System.err.println("ActivityCoordinator: onFinish  " + activity.getDescription());
  }
  
  synchronized public void waitForTermination(long timeout) throws InterruptedException {
    wait(timeout);
  }
  
  public void shutdown() {
    registryListener.close();
  }
  
  protected List<ActivityStep> findNextActivitySteps(ActivityService service, Activity activity) throws RegistryException {
    List<ActivityStep> nextStepHolder = new ArrayList<>() ;
    List<ActivityStep> activitySteps = service.getActivitySteps(activity);
    for(int i = 0; i < activitySteps.size(); i++) {
      ActivityStep step = activitySteps.get(i);
      if(ActivityStep.Status.INIT.equals(step.getStatus())) {
        nextStepHolder.add(step) ;
        break;
      }
    }
    return nextStepHolder;
  }
  
  protected <T> void schedule(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws RegistryException {
    ActivityService service = ctx.getActivityService();
    service.updateActivityStepAssigned(activity, step);
    Node activityStepNode = service.getActivityStepNode(activity, step);
    registryListener.watchHeartbeat(activityStepNode, new ActivityStepNodeWatcher(ctx));
    execute(ctx, activity, step);
  }
  
  abstract protected <T> void execute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) ;
  
  static public class ActivityNodeWatcher extends NodeWatcher {
    @Override
    public void onEvent(NodeEvent event) {
      System.err.println("ActivityNodeWatcher: event = " + event.getType() + ", path = " + event.getPath());
    }
  }
  
  public class ActivityStepNodeWatcher extends NodeWatcher {
    private ActivityExecutionContext context;
    
    ActivityStepNodeWatcher(ActivityExecutionContext context) {
      this.context = context;
    }
    
    @Override
    public void onEvent(NodeEvent event) {
      //System.err.println("ActivityStepNodeWatcher: event = " + event.getType() + ", path = " + event.getPath());
      Activity activity = null ;
      ActivityStep activityStep = null ;
      try {
        Node activityStepNode = registry.get(event.getPath()).getParentNode();
        activityStep = activityStepNode.getDataAs(ActivityStep.class);
        activity = activityStepNode.getParentNode().getParentNode().getDataAs(Activity.class);
      } catch (RegistryException e) {
        e.printStackTrace();
        return;
      }
      
      try {
        if(event.getType() == NodeEvent.Type.CREATE) {
          onExecuting(context, activity, activityStep);
        } else if(event.getType() == NodeEvent.Type.DELETE) {
          if(activityStep.getStatus() !=  ActivityStep.Status.FINISHED) {
            onBroken(context, activity, activityStep);
          } else {
            onFinish(context, activity, activityStep);
          }
        } else {
          System.err.println("ActivityStepNodeWatcher Error: event = " + event.getType() + ", path = " + event.getPath());
        }
      } catch(Exception ex) {
        ex.printStackTrace();
      }
    }
  }
}