package com.neverwinterdp.vm;

abstract public class VMApplication {
  private VM vm;
  
  public VM getVM() { return this.vm ; }
  public void setVM(VM vm) { this.vm = vm ; }
  
  abstract public void run(String[] args) throws Exception ;
}
