package com.neverwinterdp.registry.activity;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.google.inject.Injector;
import com.google.inject.Singleton;


public class HelloActivityBuilder extends ActivityBuilder {
  
  public Activity build() {
    Activity activity = new Activity();
    activity.setDescription("Hello Activity");
    activity.setType("hello");
    activity.withCoordinator(HelloActivityCoordinator.class);
    activity.withActivityStepBuilder(HelloActivityStepBuilder.class);
    return activity;
  }
  
  static public class HelloActivityStepBuilder implements ActivityStepBuilder {
    @Override
    public List<ActivityStep> build(Activity activity, Injector container) throws Exception {
      List<ActivityStep> steps = new ArrayList<>();
      Random rand = new Random() ;
      for(int i = 0; i < 10; i++) {
        if(rand.nextInt() % 2 == 0) {
          steps.add(newHelloStep("hello-step")) ;
        } else {
          steps.add(newPauseStep("pause-step")) ;
        }
      }
      return steps;
    }
    
    ActivityStep newHelloStep(String name) {
      ActivityStep step = new ActivityStep().withType(name).withExecutor(HelloActivityStepExecutor.class);
      return step ;
    }
    
    ActivityStep newPauseStep(String name) {
      ActivityStep step = 
        new ActivityStep().withType(name).withExecutor(PauseActivityExecutor.class);
      return step ;
    }
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
