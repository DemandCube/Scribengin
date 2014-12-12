package com.neverwinterdp.scribengin.stream.sink;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import com.neverwinterdp.scribengin.tuple.Tuple;

// TODO: Auto-generated Javadoc
/**
 * The Class Buffer.
 */
public abstract class Buffer {
  
  /**
   * The Enum State.
   */
  public enum State {
    
    /** The Ready. */
    Ready, 
 /** The Appending. */
 Appending, 
 /** The Full. */
 Full, 
 /** The Purging. */
 Purging;
    
  }

  /** The max buffer size. */
  private int maxBufferSize;

  /** The max buffering time. */
  private int maxBufferingTime;

  /** The max tuples. */
  private long maxTuples;

  /** The start time. */
  private long startTime;
  
  


  /**
   * The Constructor.
   *
   * @param maxDiskBufferSize the max disk buffer size
   * @param maxDiskBufferingTime the max disk buffering time
   * @param maxTuplesInDisk the max tuples in disk
   */
  public Buffer(int maxDiskBufferSize, int maxDiskBufferingTime, long maxTuplesInDisk) {
    super();
    this.maxBufferSize = maxDiskBufferSize;
    this.maxBufferingTime = maxDiskBufferingTime;
    this.maxTuples = maxTuplesInDisk;
  }

  /** The state. */
  protected State state = State.Ready;

  /**
   * Adds the Tuple to buffer.
   *
   * @param tuple the tuple
   */
  public abstract void add(Tuple tuple);

  /**
   * Gets the tuples count.
   *
   * @return the tuples count
   */
  public abstract int getTuplesCount();

  /**
   * Gets the tuples size.
   *
   * @return the tuples size
   */
  public abstract int getTuplesSize();

  /**
   * Clean.
   */
  public abstract void clean();

  /**
   * Gets the state.
   *
   * @return the state
   */
  public State getState() {
    return state;
  }

  /**
   * Check state.
   */
  public void updateState() {

    if (startTime == 0) {
      startTime = System.currentTimeMillis();
    }
    if (getTuplesCount() >= maxTuples || getTuplesSize() > maxBufferSize
        || getDuration() > maxBufferingTime) {
      state = State.Full;
    }
  }

  /**
   * Gets the duration.
   *
   * @return the duration
   */
  public long getDuration() {
    return System.currentTimeMillis() - startTime;
  }

}
