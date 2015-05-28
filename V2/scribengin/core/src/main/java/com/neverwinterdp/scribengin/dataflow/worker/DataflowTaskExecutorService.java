package com.neverwinterdp.scribengin.dataflow.worker;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mycila.jmx.annotation.JmxBean;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeEventWatcher;
import com.neverwinterdp.registry.notification.Notifier;
import com.neverwinterdp.scribengin.dataflow.DataflowContainer;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.event.DataflowEvent;
import com.neverwinterdp.util.LoggerFactory;
import com.neverwinterdp.vm.VMDescriptor;

@Singleton
@JmxBean("role=dataflow-worker, type=DataflowTaskExecutorService, dataflowName=DataflowTaskExecutorService")
public class DataflowTaskExecutorService {
  private Logger logger ;

  @Inject
  private DataflowContainer container;
  
  @Inject
  private VMDescriptor vmDescriptor ;

  private Notifier notifier ;
  private DataflowWorkerEventListenter dataflowTaskEventListener ;
  private DataflowDescriptor dataflowDescriptor;
  private List<DataflowTaskExecutor> taskExecutors;
  private DataflowWorkerStatus workerStatus = DataflowWorkerStatus.INIT;
  private boolean kill = false ;
  
  @Inject
  public void onInject(LoggerFactory lfactory, DataflowRegistry dflRegistry) throws Exception {
    logger = lfactory.getLogger(DataflowTaskExecutorService.class) ;
    logger.info("Start onInit()");
    Node workerNode = dflRegistry.getWorkerNode(vmDescriptor.getId()) ;
    notifier = new Notifier(dflRegistry.getRegistry(),  workerNode.getPath() + "/notification", "dataflow-executor-service");
    
    dataflowTaskEventListener = new DataflowWorkerEventListenter(dflRegistry);
    dataflowDescriptor = dflRegistry.getDataflowDescriptor();
    
    int numOfExecutors = dataflowDescriptor.getNumberOfExecutorsPerWorker();
    taskExecutors = new ArrayList<DataflowTaskExecutor>();
    for(int i = 0; i < numOfExecutors; i++) {
      DataflowTaskExecutorDescriptor descriptor = new DataflowTaskExecutorDescriptor ("executor-" + i);
      DataflowTaskExecutor executor = new DataflowTaskExecutor(descriptor, container);
      taskExecutors.add(executor);
    }
    logger.info("Finish onInit()");
  }
  
  public void start() throws Exception {
    logger.info("Start start()");
    notifier.info("start-start", "DataflowTaskExecutorService: start start()");
    for(int i = 0; i < taskExecutors.size(); i++) {
      DataflowTaskExecutor executor = taskExecutors.get(i);
      executor.start();
    }
    workerStatus = DataflowWorkerStatus.RUNNING;
    container.getDataflowRegistry().setWorkerStatus(container.getVMDescriptor(), workerStatus);
    notifier.info("finish-start", "DataflowTaskExecutorService: finish start()");
    logger.info("Finish start()");
  }
  
  
  void interrupt() throws Exception {
    for(DataflowTaskExecutor sel : taskExecutors) {
      if(sel.isAlive()) sel.interrupt();
    }
    waitForExecutorTermination(500);
  }
  
  public void pause() throws Exception {
    logger.info("start pause()");
    notifier.info("start-pause", "DataflowTaskExecutorService: start pause()");
    workerStatus = DataflowWorkerStatus.PAUSING;
    container.getDataflowRegistry().setWorkerStatus(container.getVMDescriptor(), workerStatus);
    interrupt();
    workerStatus = DataflowWorkerStatus.PAUSE;
    container.getDataflowRegistry().setWorkerStatus(container.getVMDescriptor(), workerStatus);
    notifier.info("finish-pause", "DataflowTaskExecutorService: finish pause()");
    logger.info("finish pause()");
  }
 
  @PreDestroy
  public void shutdown() throws Exception {
    if(kill) return;
    logger.info("Start shutdown()");
    notifier.info("start-shutdown", "DataflowTaskExecutorService: start shutdown()");
    if(workerStatus != DataflowWorkerStatus.TERMINATED) {
      System.err.println("DataflowTaskExecutorService: shutdown()");
      workerStatus = DataflowWorkerStatus.TERMINATING;
      container.getDataflowRegistry().setWorkerStatus(container.getVMDescriptor(), workerStatus);
      dataflowTaskEventListener.setComplete();
      interrupt() ;
      workerStatus = DataflowWorkerStatus.TERMINATED;
      container.getDataflowRegistry().setWorkerStatus(container.getVMDescriptor(), workerStatus);
      System.err.println("DataflowTaskExecutorService: shutdown() done!");
    }
    notifier.info("finish-shutdown", "DataflowTaskExecutorService: finish shutdown()");
    logger.info("Finish shutdown()");
  }
  
  public void simulateKill() throws Exception {
    logger.info("Start kill()");
    notifier.info("start-simulate-kill", "DataflowTaskExecutorService: start simulateKill()");
    kill = true ;
    if(workerStatus != DataflowWorkerStatus.TERMINATED) {
      for(DataflowTaskExecutor sel : taskExecutors) {
        if(sel.isAlive()) sel.kill();
      }
    }
    notifier.info("finish-simulate-kill", "DataflowTaskExecutorService: finish simulateKill()");
    logger.info("Finish kill()");
  }
  
  public boolean isAlive() {
    for(DataflowTaskExecutor sel : taskExecutors) {
      if(sel.isAlive()) {
        return true;
      }
    }
    return false;
  }
  
  synchronized void waitForExecutorTermination(long checkPeriod) throws InterruptedException {
    while(isAlive()) {
      wait(checkPeriod);
    }
  }
  
  synchronized public void waitForTerminated(long checkPeriod) throws InterruptedException, RegistryException {
    while(workerStatus != DataflowWorkerStatus.TERMINATED) {
      waitForExecutorTermination(checkPeriod);
      if(workerStatus == DataflowWorkerStatus.RUNNING) {
        workerStatus = DataflowWorkerStatus.TERMINATED;
        container.getDataflowRegistry().setWorkerStatus(container.getVMDescriptor(), workerStatus);
      }
      wait(checkPeriod);
    }
  }
  
  public class DataflowWorkerEventListenter extends NodeEventWatcher {
    public DataflowWorkerEventListenter(DataflowRegistry dflRegistry) throws RegistryException {
      super(dflRegistry.getRegistry(), true/*persistent*/);
      watchModify(dflRegistry.getWorkerEventNode().getPath());
    }

    @Override
    public void processNodeEvent(NodeEvent event) throws Exception {
      if(event.getType() == NodeEvent.Type.MODIFY) {
        DataflowEvent taskEvent = getRegistry().getDataAs(event.getPath(), DataflowEvent.class);
        if(taskEvent == DataflowEvent.PAUSE) {
          logger.info("Dataflow worker detect pause event!");
          pause() ;
        } else if(taskEvent == DataflowEvent.STOP) {
          logger.info("Dataflow worker detect stop event!");
          shutdown() ;
        } else if(taskEvent == DataflowEvent.RESUME) {
          logger.info("Dataflow worker detect resume event!");
          start() ;
        }
      }
    }
  }
}