package com.neverwinter.scribengin.utils;

import java.util.Date;
import java.util.Random;

import org.apache.log4j.Logger;

import com.neverwinter.scribengin.ScribenginFlowMaster;
import com.neverwinter.scribengin.ScribenginMain;
import com.neverwinter.scribengin.zookeeper.ZookeeperHelper;



// used for testing
public class TestUtil implements Runnable {


  private static final Logger logger = Logger.getLogger(TestUtil.class);
  private ScribenginFlowMaster flowMaster;
  private ZookeeperHelper zkHelper;

  public TestUtil(ScribenginFlowMaster flowMaster) throws InterruptedException {
    this.flowMaster = flowMaster;
    zkHelper = new ZookeeperHelper(ScribenginUtils.getZookeeperServers(ScribenginMain.props));
  }

  final Random rand = new Random();


  @Override
  public void run() {
    logger.debug("Bullying ..>>>>>>>>....>>>..>>>");
    ConfigurationCommand command = new ConfigurationCommand();
    command.setMemberName("127.0.0." + rand.nextInt(256));
    command.setObeyed(rand.nextBoolean());
    command.setTimestamp(new Date().toString());

    try {
      zkHelper.writeData("/scribengin/config", ScribenginUtils.toJson(command).getBytes());
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    boolean running = rand.nextBoolean() & rand.nextBoolean();
    logger.debug("Bullying FlowMaster " + running);
    flowMaster.setRunning(running);
  }
}
