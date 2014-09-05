package com.neverwinterdp.scribengin.buffers;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;

import org.apache.commons.chain.Chain;
import org.apache.commons.chain.Context;
import org.apache.log4j.Logger;

import com.neverwinterdp.scribengin.ScribenginContext;


// buffers until we get to a set number of messages
// useful for "one write to destination for every 10 reader threads"
public class ThreadBuffer implements Buffer<Integer> {


  public class Updater implements Runnable {

    @Override
    public void run() {
      // int integer = (Integer) context.get("integer");
      logger.debug("HAHAHA");
      //put in queue of ints
      //buffer(integer);
      //put in scribenginContext
      scribenginContext.put("integer", queue);
    }

  }

  private static final Logger logger = Logger.getLogger(ThreadBuffer.class);
  private static CyclicBarrier barrier;
  private static ScribenginContext scribenginContext;
  //shoudl this be synchronized
  private static Queue<Integer> queue;


  public ThreadBuffer(CyclicBarrier barrier2) {
    super();
    barrier = barrier2;
    scribenginContext = new ScribenginContext();
    queue = new ConcurrentLinkedQueue<Integer>();
  }

  public ThreadBuffer() {
    // TODO Auto-generated constructor stub
  }

  @Override
  public boolean postprocess(Context context, Exception exception) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean execute(Context context) throws Exception {
    int integer = (Integer) context.get("integer");
    logger.debug("Counter " + integer);
    if (barrier.await() != 0) {
      return Chain.PROCESSING_COMPLETE;
    } else {
      buffer(integer);
      logger.debug("Queue " + queue);
      context.putAll(scribenginContext);
      scribenginContext.clear();
      queue.clear();
      return Chain.CONTINUE_PROCESSING;
    }

  }

  @Override
  public Queue<Integer> buffer(Integer integer) {
    queue.add(integer);
    return queue;
  }
}
