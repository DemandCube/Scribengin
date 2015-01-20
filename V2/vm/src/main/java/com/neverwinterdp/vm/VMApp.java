package com.neverwinterdp.vm;

abstract public class VMApp {
  private VM vm;
  
  public VM getVM() { return this.vm ; }
  public void setVM(VM vm) { this.vm = vm ; }
  
  abstract public void run() throws Exception ;
  
  synchronized public void waitForShutdown() throws InterruptedException {
    wait(0);
  }
  
  synchronized public void notifyShutdown() {
    notify();
  }
}