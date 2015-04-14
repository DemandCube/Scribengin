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
  
  public void onStart(ActivityService service, Activity activity) throws RegistryException {
    Node activityNode = service.getActivityNode(activity);
    //registryListener.watch(activityNode.getPath(), new ActivityNodeWatcher());
    List<ActivityStep> nextSteps = findNextActivitySteps(service, activity);
    for(int i = 0; i < nextSteps.size(); i++) {
      ActivityStep nextStep = nextSteps.get(i);
      nextStep.setStatus(ActivityStep.Status.ASSIGNED);
      schedule(service, activity, nextStep);
    }
    //System.err.println("ActivityCoordinator: onStart  " + activity.getDescription());
  }
  
  public void onResume(ActivityService service, Activity activity) {
    
  }
  
  public void onExecuting(ActivityService service, Activity activity, ActivityStep step) {
  }
  
  public void onBroken(ActivityService service, Activity activity, ActivityStep step) throws RegistryException {
    schedule(service, activity, step);
  }
  
  
  public void onFinish(ActivityService service, Activity activity, ActivityStep step) throws RegistryException {
    List<ActivityStep> nextSteps = findNextActivitySteps(service, activity);
    if(nextSteps.size() > 0) {
      for(int i = 0; i < nextSteps.size(); i++) {
        ActivityStep nextStep = nextSteps.get(i);
        nextStep.setStatus(ActivityStep.Status.ASSIGNED);
        schedule(service, activity, nextStep);
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
  
  protected <T> void schedule(ActivityService service, Activity activity, ActivityStep step) throws RegistryException {
    service.updateActivityStepAssigned(activity, step);
    Node activityStepNode = service.getActivityStepNode(activity, step);
    registryListener.watchHeartbeat(activityStepNode, new ActivityStepNodeWatcher(service));
    execute(service, activity, step);
  }
  
  abstract protected <T> void execute(ActivityService service, Activity activity, ActivityStep step) ;
  
  static public class ActivityNodeWatcher extends NodeWatcher {
    @Override
    public void onEvent(NodeEvent event) {
      System.err.println("ActivityNodeWatcher: event = " + event.getType() + ", path = " + event.getPath());
    }
  }
  
  public class ActivityStepNodeWatcher extends NodeWatcher {
    private ActivityService service;
    
    ActivityStepNodeWatcher(ActivityService service) {
      this.service = service;
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
          onExecuting(service, activity, activityStep);
        } else if(event.getType() == NodeEvent.Type.DELETE) {
          if(activityStep.getStatus() !=  ActivityStep.Status.FINISHED) {
            onBroken(service, activity, activityStep);
          } else {
            onFinish(service, activity, activityStep);
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