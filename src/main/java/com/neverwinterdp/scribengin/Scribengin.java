/**
 * 
 */
package com.neverwinterdp.scribengin;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.neverwinterdp.scribengin.configuration.DynamicConfigurator;
import com.neverwinterdp.scribengin.utils.PropertyUtils;
import com.neverwinterdp.scribengin.utils.ScribenginUtils;
import com.neverwinterdp.scribengin.utils.TestUtil;
import com.neverwinterdp.scribengin.zookeeper.ZookeeperClusterMember;

/**
 * This is where we start.
 * 
 * @author Anthony
 * 
 */
// dynamic configuration
// cluster member on a separate thread
// TODO when deregisterd from cluster stop flow master.
public class Scribengin {

  private static final Logger logger = Logger.getLogger(Scribengin.class);
  public static Properties props;
  private ExecutorService executorService;
  private ScribenginFlowMaster flowMaster;
  private DynamicConfigurator configurator;
  private ScribenginContext scribenginContext;
  private int numThreads;
  
  @Inject @Named("scribengin:ExampleParam2")
  private String exampleParam = "DEFAULT!?!?!" ;
  
  
  public Scribengin(){
    logger.info("SCRIBENGIN IS STARTING!!!!!!!!!!!!!!!!!!!!!!!!!!");
    logger.info(exampleParam);
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {

    // BasicConfigurator.configure();

    Scribengin scribenginRunner = new Scribengin();
    props = PropertyUtils.getPropertyFile("server.properties");

    logger.debug("Properties " + props);

    scribenginRunner.numThreads = Integer.parseInt(props.getProperty("num.threads"));

    ThreadFactory threadFactory =
        new ThreadFactoryBuilder().setNameFormat("Scribengin-tributary-%d").build();
    scribenginRunner.executorService =
        Executors.newFixedThreadPool(scribenginRunner.numThreads + 2, threadFactory);

    scribenginRunner.startClusterRegistration();
    scribenginRunner.startDynamicConfigurator();

    scribenginRunner.scribenginContext = scribenginRunner.configurator.getContext();
    scribenginRunner.scribenginContext.setProps(props);
    //TODO externalize
    scribenginRunner.scribenginContext.setHDFSPath("/tmp/hdfs/path");

    scribenginRunner.startFlowMaster();

    scribenginRunner.bully();
    scribenginRunner.executorService.shutdown();

  }

  private void startDynamicConfigurator() throws Exception {
    String zkConnectString = ScribenginUtils.getZookeeperServers(props);
    String zkPath = props.getProperty("zkConfigPath");
    logger.debug("zkPath " + zkPath);
    configurator = new DynamicConfigurator(zkConnectString, zkPath);
    configurator.startWatching();
  }

  private void bully() throws InterruptedException {
    ScheduledExecutorService scheduler = Executors
        .newSingleThreadScheduledExecutor();

    // every x seconds we fire
    int x = 5;
    final ScheduledFuture<?> timeHandle = scheduler.scheduleAtFixedRate(
        new TestUtil(flowMaster), 0, x, TimeUnit.SECONDS);

    // Schedule the event, and run for 1 hour (60 * 60 seconds)
    scheduler.schedule(new Runnable() {
      public void run() {
        timeHandle.cancel(false);
      }
    }, 60 * 60, TimeUnit.SECONDS);

  }

  private boolean startClusterRegistration() {
    String zkConnectString = props.getProperty("zookeeper.server");
    logger.debug("Starting " + zkConnectString);
    String zkPath = props.getProperty("zkRegistrationPath");
    logger.debug("zkPath " + zkPath);
    ZookeeperClusterMember server = new ZookeeperClusterMember(
        zkConnectString, zkPath, "newone");

    executorService.execute(server);
    return server.state() == Service.State.RUNNING;

  }

  private void startFlowMaster() {

    int threadsToWaitOn = Integer.parseInt(props.getProperty("num.barrier.threads"));
    for (int i = 0; i <= numThreads; i++) {
      flowMaster = new ScribenginFlowMaster(scribenginContext, threadsToWaitOn, i);
      flowMaster.setRunning(true);
      executorService.execute(flowMaster);
    }
  }
}
