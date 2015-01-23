package com.neverwinterdp.vm;

abstract public class VMApp {
  private VM vm;
  private boolean waitForShutdown = false ;
  
  public VM getVM() { return this.vm ; }
  public void setVM(VM vm) { this.vm = vm ; }
  
  abstract public void run() throws Exception ;
  
  public boolean isWaittingForShutdown() { return waitForShutdown ; }
  
  synchronized public void waitForShutdown() throws InterruptedException {
    waitForShutdown = true;
    wait(0);
  }
  
  synchronized public void notifyShutdown() {
    notify();
  }
}