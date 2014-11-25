package com.neverwinterdp.vm;


public class VMApplicationDummy extends VMApplication {
  @Override
  public void run(String[] args) throws Exception {
    String name = args[0];
    try {
      while(true) {
        Thread.sleep(1000);
        System.out.println("Hello " + name);
      }
    } catch(InterruptedException ex) {
      
    }
  }
}