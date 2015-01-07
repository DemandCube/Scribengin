package com.neverwinterdp.vm;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.neverwinterdp.module.AppModule;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.command.VMCommandWatcher;

public class VM {
  static {
    System.setProperty("java.net.preferIPv4Stack", "true") ;
  }
  
  static private Map<String, VM> vms = new ConcurrentHashMap<String, VM>() ;
  
  private Logger logger = LoggerFactory.getLogger(VM.class);
  
  private VMDescriptor descriptor ;
  private VMStatus     vmStatus = VMStatus.INIT;
  
  private Injector vmContainer;
  private VMRegistry vmRegistry ;
  private VMApplicationRunner vmApplicationRunner ;
  
  public VM(VMConfig vmConfig) throws Exception {
    logger.info("Create VM with VMConfig:");
    logger.info(JSONSerializer.INSTANCE.toString(vmConfig));
    vmContainer = createVMContainer(vmConfig);
    vmRegistry = vmContainer.getInstance(VMRegistry.class);
    if(vmConfig.isSelfRegistration()) {
      logger.info("Start self registration with the registry");
      descriptor = new VMDescriptor(vmConfig);
      Registry registry = vmContainer.getInstance(Registry.class);
      VMService.register(registry, descriptor);
      logger.info("Finish self registration with the registry");
    } else {
      descriptor = vmRegistry.getVMDescriptor();
    }
    watchCommand();
    setVMStatus(VMStatus.INIT);
  }
  
  public VM(VMDescriptor vmDescriptor) throws RegistryException {
    descriptor = vmDescriptor ;
    vmContainer = createVMContainer(vmDescriptor.getVmConfig());
    vmRegistry = vmContainer.getInstance(VMRegistry.class);
    watchCommand();
    setVMStatus(VMStatus.INIT);
  }
  
  private Injector createVMContainer(final VMConfig vmConfig) {
    logger.info("Start createVMContainer(...)");
    Map<String, String> props = new HashMap<String, String>();
    props.put("vm.registry.allocated.path", VMService.ALLOCATED_PATH + "/" + vmConfig.getName());
    AppModule module = new AppModule(props) {
      @Override
      protected void configure(Map<String, String> props) {
        bindInstance(VMConfig.class, vmConfig);
        RegistryConfig rConfig = vmConfig.getRegistryConfig();
        bindInstance(RegistryConfig.class, rConfig);
        try {
          bindType(Registry.class, rConfig.getRegistryImplementation());
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    };
    logger.info("Finish createVMContainer(...)");
    return Guice.createInjector(module);
  }
  
  public VMStatus getVMStatus() { return this.vmStatus ; }

  public VMDescriptor getDescriptor() { return descriptor; }
  
  public VMApp getVMApplication() {
    if(vmApplicationRunner == null) return null;
    return vmApplicationRunner.vmApplication;
  }
  
  public VMRegistry getVMRegistry() { return this.vmRegistry ; }

  public void setVMStatus(VMStatus status) throws RegistryException {
    vmStatus = status;
    vmRegistry.updateStatus(status);
  }
  
  
  public void watchCommand() throws RegistryException {
    logger.info("Start watchCommand(...)");
    VMCommandWatcher vmCommandWatcher = new VMCommandWatcher(this);
    vmRegistry.addCommandWatcher(vmCommandWatcher);
    vmRegistry.createHeartbeat();
    logger.info("Finish watchCommand(...)");
  }
  
  public void run() throws Exception {
    logger.info("Start appStart()");
    if(vmApplicationRunner != null) {
      throw new Exception("VM Application is already started");
    }
    VMConfig vmConfig = descriptor.getVmConfig();
    Class<VMApp> vmAppType = (Class<VMApp>)Class.forName(vmConfig.getVmApplication()) ;
    VMApp vmApp = vmAppType.newInstance();
    vmApp.setVM(this);
    setVMStatus(VMStatus.RUNNING);
    vmApplicationRunner = new VMApplicationRunner(vmApp, vmConfig.getProperties()) ;
    vmApplicationRunner.start();
    logger.info("Finish appStart()");
  }
  
  public void shutdown() throws Exception {
    if(vmApplicationRunner == null || !vmApplicationRunner.isAlive()) return;
    Thread thread = new Thread() {
      public void run() {
        try {
          Thread.sleep(500);
          vmApplicationRunner.interrupt();
        } catch (InterruptedException e) {
        }
      }
    };
    thread.start();
  }

  public synchronized void notifyComplete() {
    notifyAll();
  }
  
  public synchronized void waitForComplete() throws InterruptedException {
    wait();
  }
  
  public class VMApplicationRunner extends Thread {
    VMApp vmApplication;
    Map<String, String> properties;
    
    public VMApplicationRunner(VMApp vmApplication, Map<String, String> props) {
      this.vmApplication = vmApplication;
      this.properties = props;
    }
    
    public void run() {
      try {
        vmApplication.run();
      } catch (InterruptedException e) {
      } catch (Exception e) {
        logger.error("Error in vm application", e);
      } finally {
        try {
          setVMStatus(VMStatus.TERMINATED);
        } catch (RegistryException e) {
          logger.error("Error in vm registry", e);
        }
        try {
          vmRegistry.getRegistry().disconnect();
        } catch (RegistryException e) {
          e.printStackTrace();
        }
        notifyComplete();
      }
    }
  }
  
  static public VM getVM(VMDescriptor descriptor) {
    return vms.get(descriptor.getId());
  }
  
  static public void trackVM(VM vm) {
    vms.put(vm.getDescriptor().getId(), vm);
  }
  
  static public VM run(String[] args) throws Exception {
    VMConfig vmConfig = new VMConfig();
    new JCommander(vmConfig, args);
    VM vm = new VM(vmConfig);
    vm.run();
    return vm;
  }
  
  static public VM run(VMConfig vmConfig) throws Exception {
    VM vm = new VM(vmConfig);
    vm.run();
    return vm;
  }
  
  static public void main(String[] args) throws Exception {
    System.out.println("VM: main(..) start");
    VM vm = run(args);
    vm.waitForComplete();
    System.out.println("VM: main(..) finish");
  }
}