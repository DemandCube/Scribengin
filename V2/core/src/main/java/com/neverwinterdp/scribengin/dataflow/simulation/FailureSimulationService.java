package com.neverwinterdp.scribengin.dataflow.simulation;

import javax.annotation.PostConstruct;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeEventWatcher;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.VMConfig;

@Singleton
public class FailureSimulationService {
  @Inject
  private DataflowRegistry        dflRegistry;
  
  @Inject
  private VMConfig                vmConfig;

  private FailureEventNodeWatcher failureEventNodeWatcher;
  private FailureConfig           currentFailureConfig;
  
  
  @PostConstruct
  public void onInit() throws Exception {
    failureEventNodeWatcher = new FailureEventNodeWatcher(dflRegistry);
  }
  
  public void checkFailureSimulationRequest(Activity activity, ActivityStep step) throws Exception {
    if(activity.getType().equals(currentFailureConfig.getActivityType()) &&
       step.getType().equals(currentFailureConfig.getActivityStepType())) {
      currentFailureConfig = null ;
    }
  }
  
  public class FailureEventNodeWatcher extends NodeEventWatcher {
    public FailureEventNodeWatcher(DataflowRegistry dflRegistry) throws RegistryException {
      super(dflRegistry.getRegistry(), true);
      watchModify(dflRegistry.getFailureEventNode().getPath());
    }

    @Override
    public void processNodeEvent(NodeEvent event) throws Exception {
      System.err.println("FailureSimulationService: event = " + event.getType() + ", path = " + event.getPath());
      if(event.getType() == NodeEvent.Type.MODIFY) {
        Registry registry = getRegistry() ;
        currentFailureConfig = registry.getDataAs(event.getPath(), FailureConfig.class);
        System.err.println(JSONSerializer.INSTANCE.toString(currentFailureConfig));
      }
    }
  }
}
