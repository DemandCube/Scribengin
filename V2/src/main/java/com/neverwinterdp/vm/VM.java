package com.neverwinterdp.vm;

import java.util.HashMap;
import java.util.Map;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.JCommander;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.neverwinterdp.module.AppModule;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.vm.command.VMCommandWatcher;

public class VM {
  private VMDescriptor descriptor ;
  private VMStatus     vmStatus = VMStatus.ALLOCATED;
  
  private VMRegistry vmRegistry ;
  private VMApplicationRunner vmApplicationRunner ;
  private boolean connected = false;
  
  public VM(VMConfig vmConfig) {
    descriptor = new VMDescriptor(vmConfig) ;
  }
  
  public VM(VMDescriptor vmDescriptor) {
    descriptor = vmDescriptor ;
  }
  
  public VMStatus getVMStatus() { return this.vmStatus ; }

  public VMDescriptor getDescriptor() { return descriptor; }
  
  public VMApplication getVMApplication() {
    if(vmApplicationRunner == null) return null;
    return vmApplicationRunner.vmApplication;
  }
  
  public VMRegistry getVMRegistry() { return this.vmRegistry ; }

  public void setVMStatus(VMStatus status) throws RegistryException {
    this.vmStatus = status;
    if(connected) {
      vmRegistry.updateStatus(status);
    }
  }
  
  public void connect(VMRegistry vmRegistry) throws RegistryException {
    this.vmRegistry = vmRegistry;
    connect();
  }
  
  public void connect() throws RegistryException {
    if(connected) return;
    descriptor = vmRegistry.getVMDescriptor();
    VMCommandWatcher vmCommandWatcher = new VMCommandWatcher(this);
    vmRegistry.addCommandWatcher(vmCommandWatcher);
    vmRegistry.createHeartbeat();
    vmRegistry.updateStatus(vmStatus);
    connected = true;
  }
  
  public void appStart(VMApplication vmApp, Map<String, String> props) throws Exception{
    if(vmApplicationRunner != null) {
      throw new Exception("VM Application is already started");
    }
    vmApp.setVM(this);
    setVMStatus(VMStatus.RUNNING);
    vmApplicationRunner = new VMApplicationRunner(vmApp, props) ;
    vmApplicationRunner.start();
  }
  
  public void appStart(String vmAppClass, Map<String, String> props) throws Exception {
    Class<VMApplication> vmAppType = (Class<VMApplication>)Class.forName(vmAppClass) ;
    VMApplication vmApp = vmAppType.newInstance();
    connect();
    appStart(vmApp, props);
  }
  
  public void appStop() throws Exception {
    if(vmApplicationRunner == null) return;
    vmApplicationRunner.interrupt();
    vmRegistry.updateStatus(VMStatus.TERMINATED);
  }
  
  public void exit() throws Exception {
    appStop();
    vmRegistry.deleteHeartbeat();
  }
  
  static public class VMApplicationRunner extends Thread {
    private VMApplication vmApplication;
    private Map<String, String> properties;
    
    public VMApplicationRunner(VMApplication vmApplication, Map<String, String> props) {
      this.vmApplication = vmApplication;
      this.properties = props;
    }
    
    public void run() {
      try {
        vmApplication.onInit(properties);
        vmApplication.run();
        vmApplication.onDestroy();
      } catch (InterruptedException e) {
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
  
  static public class VMArgs {
    @DynamicParameter(names = "-P", description = "Dynamic properties")
    Map<String, String> properties = new HashMap<String, String>();
  }
  
  static public void main(String[] args) throws Exception {
    VMArgs vmArgs = new VMArgs() ;
    new JCommander(vmArgs, args);
    AppModule module = new AppModule(vmArgs.properties) ;
    Injector container = Guice.createInjector(module);
    VM vm = container.getInstance(VM.class) ;
  }
}