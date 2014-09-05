package com.neverwinter.scribengin.impl;

import java.util.Queue;

import org.apache.commons.chain.Context;

import com.neverwinter.scribengin.api.Buffer;


// buffers until set time elapses
// useful for "write to destination every 60 seconds"
public class TimeBuffer implements Buffer<byte[]> {

  //No of milliseconds to hold data
  private int milliseconds;

  public TimeBuffer(int milliseconds) {
    super();
    this.milliseconds = milliseconds;
  }

  //if time is passed move to next step
  //else go back for more 
  @Override
  public boolean execute(Context context) throws Exception {
    // TODO if time is reached put something in context and proceed

    //else put something in context  and go back to start.
    return false;
  }


  //go for more messages
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

  public int getMilliseconds() {
    return milliseconds;
  }

  public void setMilliseconds(int milliseconds) {
    this.milliseconds = milliseconds;
  }

  @Override
  public String toString() {
    return "TimeBuffer [milliseconds=" + milliseconds + "]";
  }


}
