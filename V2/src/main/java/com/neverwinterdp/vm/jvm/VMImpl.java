package com.neverwinterdp.vm.jvm;

import java.util.HashMap;
import java.util.Map;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.JCommander;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.neverwinterdp.module.AppModule;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.vm.VM;
import com.neverwinterdp.vm.VMApplication;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMRegistry;
import com.neverwinterdp.vm.command.VMCommandWatcher;

@Singleton
public class VMImpl implements VM {
  private VMDescriptor descriptor ;
  
  private VMRegistry vmRegistry ;
  private VMApplicationRunner vmApplicationRunner ;
  
  @Inject
  public void init(VMRegistry vmRegistryService) throws RegistryException {
    this.vmRegistry = vmRegistryService;
    descriptor = vmRegistryService.getVMDescriptor();
    VMCommandWatcher vmCommandWatcher = new VMCommandWatcher(this);
    vmRegistry.addCommandWatcher(vmCommandWatcher);
    System.out.println("Init VM, vm registry allocated path = " + descriptor.getStoredPath());
  }
  
  @Override
  public VMDescriptor getDescriptor() { return descriptor; }

  @Override
  public VMRegistry getVMRegistry() { return this.vmRegistry ; }
  
  @Override
  public void appStart(String vmAppClass, String[] args) throws Exception {
    if(vmApplicationRunner != null) {
      throw new Exception("VM Application is already started");
    }
    Class<VMApplication> vmAppType = (Class<VMApplication>)Class.forName(vmAppClass) ;
    VMApplication vmApp = vmAppType.newInstance();
    vmApplicationRunner = new VMApplicationRunner(vmApp, args) ;
    vmApplicationRunner.start();
  }
  
  @Override
  public void appStop() throws Exception {
    if(vmApplicationRunner == null) return;
    vmApplicationRunner.interrupt();
  }
  
  @Override
  public void exit() throws Exception {
    appStop();
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
  
  static public class VMArgs {
    @DynamicParameter(names = "-P", description = "Dynamic properties")
    Map<String, String> properties = new HashMap<String, String>();
  }
  
  static public VMImpl create(String[] args) throws Exception {
    VMArgs vmArgs = new VMArgs() ;
    new JCommander(vmArgs, args);
    AppModule module = new AppModule(vmArgs.properties) {
      protected void configure(Map<String, String> properties) {
        try {
          bindType(Registry.class, properties.get("registry.implementation"));
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    };
    Injector container = Guice.createInjector(module);
    VMImpl vm = container.getInstance(VMImpl.class) ;
    return vm;
  }
}
