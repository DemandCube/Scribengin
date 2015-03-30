package com.neverwinterdp.registry.activity;

import java.util.Random;

import com.google.inject.Singleton;


public class HelloActivityBuilder extends ActivityBuilder {
  public HelloActivityBuilder() {
    getActivity().setDescription("Hello Activity");
    getActivity().setType("hello");
    getActivity().withCoordinator(HelloActivityCoordinator.class);
  }
  
  public HelloActivityBuilder(int numOfStep) {
    this();
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
        withExecutor(HelloActivityStepExecutor.class));
    return this ;
  }
  
  public HelloActivityBuilder addPauseStep(String name) {
    add(new ActivityStep().
        withType(name).
        withExecutor(PauseActivityExecutor.class));
    return this ;
  }
  
  @Singleton
  static public class HelloActivityStepExecutor implements ActivityStepExecutor {
    @Override
    public void execute(Activity activity, ActivityStep step) {
      System.out.println("hello activity executor, step = " + step.getId()) ;
    }
  }
  
  @Singleton
  static public class PauseActivityExecutor implements ActivityStepExecutor {
    @Override
    public void execute(Activity activity, ActivityStep step) {
      System.out.println("pause activity executor,  step = " + step.getId() + ", pause = 3s") ;
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
      }
      System.out.println("pause activity executor,  step = " + step.getId() + ", resume") ;
    }
  }
}
