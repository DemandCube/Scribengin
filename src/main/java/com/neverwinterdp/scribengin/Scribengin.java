/**
 * 
 */
package com.neverwinterdp.scribengin;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.log4j.Logger;

import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.neverwinterdp.scribengin.configuration.DynamicConfigurator;
import com.neverwinterdp.scribengin.utils.PropertyUtils;
import com.neverwinterdp.scribengin.utils.ScribenginUtils;
import com.neverwinterdp.scribengin.zookeeper.ZookeeperClusterMember;
import com.neverwinterdp.scribengin.zookeeper.ZookeeperHelper;

/**
 * This is where we start.
 * 
 * @author Anthony
 * 
 */
// dynamic configuration
// cluster member on a separate thread
/**
 *  TODO when deregisterd from cluster stop flow master.
 */
public class Scribengin {
  private static final Logger logger = Logger.getLogger(Scribengin.class);
  public static Properties props;
  private ExecutorService executorService;
  private ScribenginFlowMaster flowMaster;
  private DynamicConfigurator configurator;
  private ScribenginContext scribenginContext;
  private int numThreads;
  private String propertyFilePath;
  private ThreadFactory threadFactory;


  public Scribengin() {
    this("src/main/resources/server.properties");
  }

  public Scribengin(String propFilePath) {
    this.propertyFilePath = propFilePath;
    props = PropertyUtils.getPropertyFile(this.propertyFilePath);
    logger.debug("Properties " + props);
  }

  public Scribengin(Properties p) {
    props.putAll(p);
    logger.debug("Properties " + props);
  }

  public void init() throws Exception {
    this.numThreads = Integer.parseInt(props.getProperty("num.threads"));
    this.threadFactory =
        new ThreadFactoryBuilder().setNameFormat("Scribengin-tributary-%d").build();
    this.executorService = Executors.newFixedThreadPool(this.numThreads + 2, threadFactory);
  }

  public void start() {
    startClusterRegistration();
    try {
      startDynamicConfigurator();
    } catch (Exception e) {
      logger.error("Could not startDynamicConfigurator");
      logger.error(e.getStackTrace());
    }

    this.scribenginContext = this.configurator.getContext();
    this.scribenginContext.setProps(props);

    //TODO externalize
    this.scribenginContext.setHDFSPath("/tmp/hdfs/path");
    try {
      this.scribenginContext
          .setZkHelper(new ZookeeperHelper(props.getProperty("zookeeper.server")));
      startFlowMaster();
    } catch (InterruptedException e) {
      logger.error("Could not start bully");
      logger.error(e.getStackTrace());
    }
  }

  public void stop() {
    this.executorService.shutdown();
  }


  private void startDynamicConfigurator() throws Exception {
    String zkConnectString = ScribenginUtils.getZookeeperServers(props);
    String zkPath = props.getProperty("zkConfigPath");
    logger.debug("zkPath " + zkPath);
    configurator = new DynamicConfigurator(zkConnectString, zkPath);
    configurator.startWatching();
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
    for (int i = 1; i <= numThreads; i++) {
      flowMaster = new ScribenginFlowMaster(scribenginContext, threadsToWaitOn, i);
      flowMaster.setRunning(true);
      executorService.execute(flowMaster);
    }
  }



  public static void main(String[] args) throws Exception {
    Scribengin x = new Scribengin();
    x.init();
    x.start();
    Thread.sleep(5000);
    x.stop();
  }
}
