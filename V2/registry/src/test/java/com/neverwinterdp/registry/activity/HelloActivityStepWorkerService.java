package com.neverwinterdp.registry.activity;

import com.google.inject.Singleton;

@Singleton
public class HelloActivityStepWorkerService extends ActivityStepWorkerService {
  private HelloActivityStepWorkerDescriptor workerDescriptor = new HelloActivityStepWorkerDescriptor(1) ;
  
  @Override
  public ActivityStepWorkerDescriptor getActivityStepWorkerDescriptor() {
    return workerDescriptor;
  }

  static public class HelloActivityStepWorkerDescriptor implements ActivityStepWorkerDescriptor {
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
