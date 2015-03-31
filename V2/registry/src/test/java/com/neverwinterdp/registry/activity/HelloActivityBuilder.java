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

    @Override
    public <T> T getWorkerInfo() { return (T) new HelloActivityStepWorkerDescriptor(1); }
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

    @Override
    public <T> T getWorkerInfo() { return (T) new HelloActivityStepWorkerDescriptor(1); }
  }
  
  static public class HelloActivityStepWorkerDescriptor {
    private int id;
    String      refPath = "some/path";

    public HelloActivityStepWorkerDescriptor() {} 
    
    public HelloActivityStepWorkerDescriptor(int id) {
      this.id = id ;
    }
    
    public int getId() { return id; }
    public void setId(int id) { this.id = id; }

    public String getRefPath() { return refPath; }
    public void setRefPath(String refPath) { this.refPath = refPath; }
  }
}
