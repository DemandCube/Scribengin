package com.neverwinterdp.registry.activity;

import java.util.Random;


public class HelloActivityBuilder extends ActivityBuilder {
  public HelloActivityBuilder() {
    getActivity().setType("hello");
    getActivity().withCoordinator(HelloActivityCoordinator.class);
  }
  
  public HelloActivityBuilder(int numOfStep) {
    getActivity().setType("hello");
    getActivity().withCoordinator(HelloActivityCoordinator.class);
    Random rand = new Random() ;
    for(int i = 0; i < numOfStep; i++) {
      if(rand.nextInt() % 2 == 0) {
        addHelloStep("hello-step") ;
      } else {
        addPauseStep("pause-step") ;
      }
    }
  }
  
  public HelloActivityBuilder addHelloStep(String name) {
    add(new ActivityStep().
        withType(name).
        withExecutor(HelloActivityExecutor.class));
    return this ;
  }
  
  public HelloActivityBuilder addPauseStep(String name) {
    add(new ActivityStep().
        withType(name).
        withExecutor(PauseActivityExecutor.class));
    return this ;
  }
  
  static public class HelloActivityExecutor implements ActivityStepExecutor {
    @Override
    public void execute(Activity activity, ActivityStep step) {
      System.out.println("hello activity executor, step = " + step.getId()) ;
    }
  }
  
  static public class PauseActivityExecutor implements ActivityStepExecutor {
    @Override
    public void execute(Activity activity, ActivityStep step) {
      System.out.println("pause activity executor,  step = " + step.getId() + ", pause = 3s") ;
      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
      }
      System.out.println("pause activity executor,  step = " + step.getId() + ", resume") ;
    }
  }
  
  static public class HelloActivityCoordinator implements ActivityCoordinator {
    @Override
    public void onStart(Activity activity) {
    }
    
    public void onResume(Activity activity) {
    }

    @Override
    public void onAssign(Activity activity, ActivityStep step) {
    }

    @Override
    public void onFinish(Activity activity, ActivityStep step) {
    }

    @Override
    public void onFinish(Activity activity) {
    }
  }
}
