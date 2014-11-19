package com.neverwinterdp.scribengin.vmresource.jvm;

import com.neverwinterdp.scribengin.vmresource.VMApplication;
import com.neverwinterdp.scribengin.vmresource.VMResource;
import com.neverwinterdp.scribengin.vmresource.VMResourceDescriptor;

public class VMResourceImpl implements VMResource {
  private VMResourceDescriptor descriptor ;
  private VMApplicationRunner vmApplicationRunner ;
  
  public VMResourceImpl(long id, int cpuCores, int memory) {
    descriptor = new VMResourceDescriptor() ;
    descriptor.setId(id);
    descriptor.setCpuCores(cpuCores);
    descriptor.setMemory(memory);
  }
  
  @Override
  public VMResourceDescriptor getDescriptor() { return descriptor; }

  @Override
  public void startApp(String vmAppClass, String[] args) throws Exception {
    if(vmApplicationRunner != null) {
      throw new Exception("VM Application is already started");
    }
    Class<VMApplication> vmAppType = (Class<VMApplication>)Class.forName(vmAppClass) ;
    VMApplication vmApp = vmAppType.newInstance();
    vmApplicationRunner = new VMApplicationRunner(vmApp, args) ;
    vmApplicationRunner.start();
  }
  
  @Override
  public void stopApp() throws Exception {
    if(vmApplicationRunner == null) return;
    vmApplicationRunner.interrupt();
  }
  
  @Override
  public void exit() throws Exception {
    stopApp();
  }
  
  static public class VMApplicationRunner extends Thread {
    private VMApplication vmApplication;
    private String[]  args ;
    
    public VMApplicationRunner(VMApplication vmApplication, String[] args) {
      this.vmApplication = vmApplication;
      this.args = args;
    }
    
    public void run() {
      try {
        vmApplication.run(args);
      } catch (InterruptedException e) {
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
