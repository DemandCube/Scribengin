package com.neverwinter.scribengin.impl;

import java.util.Date;
import java.util.Random;

import org.apache.log4j.Logger;

import com.neverwinter.scribengin.ConfigurationCommand;
import com.neverwinter.scribengin.utils.Utils;
import com.neverwinter.scribengin.zookeeper.ZookeeperHelper;



// used for testing
class Bully implements Runnable {


  private static final Logger logger = Logger.getLogger(Bully.class);
  private ScribenginFlowMaster flowMaster;
  private ZookeeperHelper zkHelper;

  Bully(ScribenginFlowMaster flowMaster) throws InterruptedException {
    this.flowMaster = flowMaster;
    zkHelper = new ZookeeperHelper(Utils.getZookeeperServers(ScribenginMain.props));
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
      zkHelper.writeData("/scribengin/config", Utils.toJson(command).getBytes());
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    boolean running = rand.nextBoolean() & rand.nextBoolean();
    logger.debug("Bullying FlowMaster " + running);
    flowMaster.setRunning(running);
  }
}
