package com.neverwinterdp.scribengin.vmresource.jvm;

import com.neverwinterdp.scribengin.vmresource.VMApplication;
import com.neverwinterdp.scribengin.vmresource.VMResource;

public class VMResourceImpl implements VMResource {
  private long id;
  private int  memory ;
  private int  cpuCores ;
  private VMApplicationRunner vmApplicationRunner ;
  
  public VMResourceImpl(long id, int cpuCores, int memory) {
    this.id = id ;
    this.memory = memory ;
    this.cpuCores = cpuCores;
  }
  
  @Override
  public long getId() { return id; }

  
  @Override
  public int getMemory() { return memory; }

  @Override
  public int getCpuCores() { return cpuCores; }

  @Override
  public String getHostname() { return "localhost"; }

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
