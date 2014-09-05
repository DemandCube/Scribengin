package com.neverwinter.scribengin.impl;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.chain.Chain;
import org.apache.commons.chain.Context;

import com.neverwinter.scribengin.api.Buffer;

// buffers until we get to a set number of messages
// useful for "wait until we read 1000 messages before writing to destination"
public class CountBuffer implements Buffer<byte[]> {


  private BlockingQueue<byte[]> queue;
  private int counter;

  public CountBuffer(int counter) {
    super();
    this.counter = counter;
    queue = new ArrayBlockingQueue<byte[]>(counter);
  }

  @Override
  public boolean execute(Context context) throws Exception {
    // if queue is full
    //proceed
    //else go for more data
    if (queue.remainingCapacity() == 0) {
      return Chain.CONTINUE_PROCESSING;
    }
    else {
      //context.put all previous stuff
      return Chain.PROCESSING_COMPLETE;
    }
  }

  @Override
  public boolean postprocess(Context context, Exception exception) {
    // TODO Auto-generated method stub
    return false;
  }


  @Override
  public Queue<byte[]> buffer(byte[] t) {
    // TODO Auto-generated method stub
    return null;
  }

  public BlockingQueue<byte[]> getQueue() {
    return queue;
  }

  public void setQueue(BlockingQueue<byte[]> queue) {
    this.queue = queue;
  }

  public int getCounter() {
    return counter;
  }

  public void setCounter(int counter) {
    this.counter = counter;
  }

}
