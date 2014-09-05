package com.neverwinter.scribengin.utils;

import java.util.Calendar;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

// Currently not in use.
public class EverlastingThread implements Callable<String> {

  private static final Logger logger = Logger
      .getLogger(EverlastingThread.class);

  private ScheduledExecutorService executorService;
  private int time;
  private TimeUnit timeUnit;

  public EverlastingThread(ScheduledExecutorService executorService,
      int time, TimeUnit timeUnit) {
    this.executorService = executorService;
    this.time = time;
    this.timeUnit = timeUnit;
  }

  public String call() throws Exception {
    logger.debug("in call()");
    try {
      // populate ProducerConsumer queue
      System.out.println("calling miami putting items in queue "
          + Calendar.getInstance().getTime());

      return "false";
    }

    finally {
      // rerun after x timeUnits
      executorService.schedule(this, time, timeUnit);
    }
  }

}
