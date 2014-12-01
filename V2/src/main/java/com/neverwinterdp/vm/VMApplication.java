package com.neverwinterdp.vm;

import java.util.Map;


abstract public class VMApplication {
  private VM vm;
  
  public VM getVM() { return this.vm ; }
  public void setVM(VM vm) { this.vm = vm ; }
  
  abstract public void onInit(Map<String, String> properties) throws Exception ;
  abstract public void onDestroy() throws Exception ;
  abstract public void run() throws Exception ;
}