package com.neverwinterdp.vm;

import java.util.Map;


public class DummyVMApplication extends VMApplication {
  private String name ;

  @Override
  public void onInit(Map<String, String> properties) throws Exception {
    name = properties.get("name") ;
  }

  @Override
  public void onDestroy() throws Exception {
  }
  
  @Override
  public void run() throws Exception {
    try {
      while(true) {
        Thread.sleep(1000);
        System.out.println("Hello " + name);
      }
    } catch(InterruptedException ex) {
      
    }
  }
}