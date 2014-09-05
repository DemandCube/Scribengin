package com.neverwinter.scribengin;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.neverwinter.scribengin.impl.DynamicConfigurator;
import com.neverwinter.scribengin.utils.Utils;
import com.neverwinter.scribengin.zookeeper.ZookeeperHelper;

public class Main {
  private static final Logger logger = Logger.getLogger(Server3.class);

  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();
    Main main = new Main();
    DynamicConfigurator configurator =
        new DynamicConfigurator("192.168.33.33:2181", "/scribengin/config");
    // configurator.startWatching();
    main.bully();
  }

  private void bully() throws InterruptedException {
    ScheduledExecutorService scheduler = Executors
        .newSingleThreadScheduledExecutor();

    // every x seconds we fire
    int x = 5;
    final ScheduledFuture<?> timeHandle = scheduler.scheduleAtFixedRate(
        new Bully(), 0, x, TimeUnit.SECONDS);

    // Schedule the event, and run for 1 hour (60 * 60 seconds)
    scheduler.schedule(new Runnable() {
      public void run() {
        timeHandle.cancel(false);
      }
    }, 60 * 60, TimeUnit.SECONDS);

  }



  class Bully implements Runnable {

    ZookeeperHelper helper;

    Bully() throws InterruptedException {

      helper = new ZookeeperHelper("192.168.33.33:2181");

    }

    final Random rand = new Random();

    @Override
    public void run() {
      logger.debug("bULLYINGSgh|gHGHG|HGHGHGHTYHJKLJNB");
      ConfigurationCommand command = new ConfigurationCommand();
      command.setMemberName("127.0.0." + rand.nextInt(256));
      command.setObeyed(rand.nextBoolean());
      command.setTimestamp(new Date().toString());

      try {
        helper.writeData("/scribengin/config", Utils.toJson(command).getBytes());
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      boolean running = rand.nextBoolean() & rand.nextBoolean();
      logger.debug("Bullying FlowMaster " + running);
      //  flowMaster.setRunning(running);
    }
  }
}
