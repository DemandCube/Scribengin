package com.neverwinterdp.vm;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import com.mycila.guice.ext.closeable.CloseableInjector;
import com.mycila.guice.ext.closeable.CloseableModule;
import com.mycila.guice.ext.jsr250.Jsr250Module;
import com.neverwinterdp.module.AppModule;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.command.VMCommandWatcher;
import com.neverwinterdp.vm.service.VMService;

public class VM {
  static {
    System.setProperty("java.net.preferIPv4Stack", "true") ;
  }
  
  static private Map<String, VM> vms = new ConcurrentHashMap<String, VM>() ;
  
  private Logger                 logger   = LoggerFactory.getLogger(VM.class);

  private VMDescriptor           descriptor;
  private VMStatus               vmStatus = VMStatus.INIT;

  private Injector               vmContainer;
  private VMRegistry             vmRegistry;
  private VMApplicationRunner    vmApplicationRunner;
  
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
    
    init();
  }
  
  public VM(VMDescriptor vmDescriptor) throws RegistryException {
    descriptor = vmDescriptor ;
    vmContainer = createVMContainer(vmDescriptor.getVmConfig());
    vmRegistry = vmContainer.getInstance(VMRegistry.class);
    
    init();
  }
  
  public Injector getVMContainer() { return this.vmContainer ; }
  
  private Injector createVMContainer(final VMConfig vmConfig) {
    logger.info("Start createVMContainer(...)");
    Map<String, String> props = new HashMap<String, String>();
    props.put("vm.registry.allocated.path", VMService.ALLOCATED_PATH + "/" + vmConfig.getName());
    AppModule module = new AppModule(props) {
      @Override
      protected void configure(Map<String, String> props) {
        try {
          bindInstance(VMConfig.class, vmConfig);
          //bindInstance(VMDescriptor.class, descriptor);
          RegistryConfig rConfig = vmConfig.getRegistryConfig();
          bindInstance(RegistryConfig.class, rConfig);

          bindType(Registry.class, rConfig.getRegistryImplementation());
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    };
    logger.info("Finish createVMContainer(...)");
    return Guice.createInjector(Stage.PRODUCTION, new CloseableModule(),new Jsr250Module(), module);
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
  
  
  public void init() throws RegistryException {
    logger.info("Start init(...)");
    setVMStatus(VMStatus.INIT);
    vmRegistry.addCommandWatcher(new VMCommandWatcher(this));
    vmRegistry.createHeartbeat();
    logger.info("Finish init(...)");
  }
  
  public void run() throws Exception {
    logger.info("Start run()");
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
    logger.info("Finish run()");
  }
  
  public void shutdown() throws Exception {
    if(vmApplicationRunner == null || !vmApplicationRunner.isAlive()) return;
    Thread thread = new Thread() {
      public void run() {
        try {
          Thread.sleep(500);
          if(vmApplicationRunner.vmApplication.isWaittingForShutdown()) {
            vmApplicationRunner.vmApplication.notifyShutdown();
          } else {
            vmApplicationRunner.interrupt();
          }
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
          vmContainer.getInstance(CloseableInjector.class).close();
        } catch (RegistryException e) {
          e.printStackTrace();
          logger.error("Error in vm registry", e);
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