package com.neverwinterdp.registry.activity;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.inject.Injector;
import com.neverwinterdp.registry.RegistryException;

public class ActivityStepWorkerService<T> {
  private T workerDescriptor ;
  private ActivityService service ;
  
  private List<ActivityStepWorker> workers = new ArrayList<>();
  private ExecutorService executorService ;
  private Random rand = new Random() ;

  public ActivityStepWorkerService() {
  }
  
  public ActivityStepWorkerService(T workerDescriptor, Injector container, String activityPath) throws RegistryException {
    init(workerDescriptor, container, activityPath);
  }

  public void init(T workerDescriptor, Injector container, String activityPath) throws RegistryException {
    this.workerDescriptor = workerDescriptor;
    int numOfWorkers = 5;
    executorService = Executors.newFixedThreadPool(numOfWorkers);
    for(int i = 0; i < numOfWorkers; i++) {
      ActivityStepWorker worker = new ActivityStepWorker() ;
      workers.add(worker);
      executorService.submit(worker);
    }
    executorService.shutdown();
    service = new ActivityService(container, activityPath);
  }

  
  public T getWorkerDescriptor() { return workerDescriptor; }
  
  public void exectute(Activity activity, ActivityStep step) {
    ActivityStepWorkUnit wUnit = new ActivityStepWorkUnit(activity, step) ;
    ActivityStepWorker worker = workers.get(rand.nextInt(workers.size()));
    worker.offer(wUnit);
  }
  
  public class ActivityStepWorker implements Runnable {
    private BlockingQueue<ActivityStepWorkUnit> activityStepWorkUnits = new LinkedBlockingQueue<>() ;
    
    public void offer(ActivityStepWorkUnit activityStepWorkUnit) {
      activityStepWorkUnits.add(activityStepWorkUnit);
    }
    
    @Override
    public void run() {
      ActivityStepWorkUnit activityStepWorkUnit  = null ; 
      try {
        while((activityStepWorkUnit = activityStepWorkUnits.take()) != null) {
          Activity activity = activityStepWorkUnit.getActivity() ;
          ActivityStep activityStep = activityStepWorkUnit.getActivityStep() ;
          Exception error = null ;
          for(int i = 0; i < activityStep.getMaxRetries(); i++) {
            error = null;
            long startTime = System.currentTimeMillis();
            try {
              service.updateActivityStepExecuting(activity, activityStep, getWorkerDescriptor());
              ActivityStepExecutor executor = 
                  service.getActivityStepExecutor(activityStepWorkUnit.getActivityStep().getExecutor());
              executor.execute(activity, activityStep);
              break ;
            } catch (Exception e) {
              activityStep.addLog("Fail to execute the activity due to the error: " + e.getMessage());
              System.err.println("Fail to execute the activity due to the error: " + e.getMessage());
              e.printStackTrace();
              error = e ;
            } finally {
              long executeTime = System.currentTimeMillis() - startTime ;
              activityStep.setExecuteTime(executeTime);
              activityStep.setTryCount(i + 1);
              service.updateActivityStepFinished(activity, activityStep);
            }
          }
          if(error != null) return ;
        }
      } catch (InterruptedException e) {
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
  }
  
  static public class ActivityStepWorkUnit {
    private Activity activity;
    private ActivityStep activityStep ;
    
    public ActivityStepWorkUnit(Activity activity, ActivityStep activityStep) {
      this.activity = activity;
      this.activityStep = activityStep;
    }
    
    public Activity getActivity() { return activity ; }
    
    public ActivityStep getActivityStep() { return activityStep ; }
  }
}
