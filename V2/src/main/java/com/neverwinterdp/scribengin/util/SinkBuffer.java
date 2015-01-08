package com.neverwinterdp.scribengin.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.neverwinterdp.scribengin.stream.sink.S3SinkConfig;
import com.neverwinterdp.scribengin.stream.sink.partitioner.SinkPartitioner;
import com.neverwinterdp.scribengin.tuple.Tuple;

/**
 * The Class SinkBuffer.
 */
public class SinkBuffer implements Iterable<Tuple> {

  /** The max tuples. */
  private long maxTuplesInMemory;

  /** The max buffer size. */
  private int maxTuplesSizeInMemory;

  /** The max buffering time. */
  private int maxBufferingTimeInMemory;

  /** The max tuples. */
  private long maxTuplesOnDisk;

  /** The max buffer size. */
  private int maxBufferSizeOnDisk;

  /** The max buffering time. */
  private int maxBufferingTimeOnDisk;

  /** The tuples count. */
  private int tuplesCountInMemory;

  /** The tuples size in memory. */
  private int tuplesSizeInMemory;

  /** The start time. */
  private long startBufferingTimeInMemory;

  /** The tuples count on disk. */
  private int tuplesCountOnDisk;

  /** The tuples size. */
  private int tuplesSizeOnDisk;

  /** The start time. */
  private long startBufferingTimeOnDisk;

  /** The buffer size. */
  private long mappedByteBufferSize;

  /** The files. */
  private LinkedList<File> files = new LinkedList<File>();

  /** The chunk size. */
  private int chunkSize;

  /** The partitioner. */

  private SinkPartitioner partitioner;

  /** The tuples chunk. */
  LinkedList<Tuple> tuplesChunk = new LinkedList<>();

  /** The memory buffering enabled. */
  private boolean memoryBufferingEnabled;

  /** The saturated. */
  private boolean saturated = false;
  /** The logger. */
  private static Logger logger;
  /** The buffer. */
  private LinkedList<Tuple> tuples = new LinkedList<Tuple>();

  /** The local tmp dir. */
  private String localTmpDir;

  private Thread bufferThread;

  private boolean active = true;

  /**
   * The Constructor.
   *
   * @param partitioner
   *          the partitioner
   * @param config
   *          the configuration
   */
  public SinkBuffer(SinkPartitioner partitioner, S3SinkConfig config) {
    this.localTmpDir = config.getLocalTmpDir();
    this.maxTuplesSizeInMemory = config.getMemoryMaxBufferSize();
    this.maxBufferingTimeInMemory = config.getMemoryMaxBufferingTime();
    this.maxTuplesOnDisk = config.getMemoryMaxTuples();
    this.maxBufferSizeOnDisk = config.getDiskMaxBufferSize();
    this.maxBufferingTimeOnDisk = config.getDiskMaxBufferingTime();
    this.maxTuplesInMemory = config.getMemoryMaxTuples();
    this.mappedByteBufferSize = config.getMappedByteBufferSize();
    this.partitioner = partitioner;
    this.chunkSize = config.getChunkSize();
    memoryBufferingEnabled = config.isMemoryBufferingEnabled();
    bufferThread = new Thread() {
      public void run() {
        try {
          runProcessLoop();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    };
    bufferThread.start();
    logger = LoggerFactory.getLogger(SinkBuffer.class);
  }

  private void setProcessLoopActive(boolean x) {
    this.active = x;
  }

  /**
   * Adds the Tuple to the buffer.
   *
   * @param tuple
   *          the tuple
   */

  public void add(Tuple tuple) {

    if (memoryBufferingEnabled) {
      if (!checkMemoryAvailability(tuple.getData().length)) {
        setProcessLoopActive(true);
        tuplesCountInMemory = 0;
        tuplesSizeInMemory = 0;
      }
      tuples.add(tuple);
      tuplesCountInMemory++;
      tuplesSizeInMemory += tuple.getData().length;
    } else {
      addToDisk(tuple);
      updateDiskState();

    }

  }

  /**
   * Adds the to disk.
   *
   * @param tuple
   *          the tuple
   * @return true, if adds the to disk
   */
  private boolean addToDisk(Tuple tuple) {

    try {
      tuplesChunk.add(tuple);
      // write every chunk of tuples in one file
      if (tuplesChunk.size() == chunkSize) {
        long startOffset = tuplesChunk.getFirst().getCommitLogEntry().getStartOffset();
        long endOffset = tuplesChunk.getLast().getCommitLogEntry().getEndOffset();
        // call partitioner to get the path of the file depending of the
        // offset
        // the path will be later used to deduce the s3 path
        String path = localTmpDir + "/" + partitioner.getPartition(startOffset, endOffset);
        // create file using the path
        File file = new File(path);
        File parent = file.getParentFile();
        if (!parent.exists() && !parent.mkdirs()) {
          throw new IllegalStateException("Couldn't create dir: " + parent);
        }
        // write a memory mapped file
        int start = 0;
        FileChannel fc = new RandomAccessFile(file, "rw").getChannel();
        for (Tuple t : tuplesChunk) {
          MappedByteBuffer mem = fc.map(FileChannel.MapMode.READ_WRITE, 0, mappedByteBufferSize);
          if (!mem.hasRemaining()) {
            start += mem.position();
            mem = fc.map(FileChannel.MapMode.READ_WRITE, start, t.getData().length);
          }
          mem.put(t.getData());
          tuplesSizeOnDisk += t.getData().length;
        }
        // add the file to the liste of file created
        files.add(file);
        tuplesChunk.clear();
        fc.close();
      }
      tuplesCountOnDisk++;

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return true;
  }

  private void runProcessLoop() throws InterruptedException {
    while (true) {
      if (active) {
        purgeMemoryToDisk();
      }
      Thread.sleep(1000);
    }
  }

  /**
   * Buffer to disk.
   */
  public void purgeMemoryToDisk() {
    logger.info("purge Memory To Disk");
    LinkedList<Tuple> tempTuples = tuples;
    tuples = new LinkedList<Tuple>();
    int mustRemain = tuples.size() % chunkSize;
    if (mustRemain != 0) {
      for (int i = mustRemain; i < 1; i--) {
        tuples.add(tempTuples.get(tempTuples.size() - i));
      }
    }
    while (tempTuples.size() > mustRemain) {
      addToDisk(tempTuples.poll());
    }
    tempTuples.clear();
    tempTuples = null;
    updateDiskState();
    setProcessLoopActive(false);

  }

  /**
   * Check memory availability.
   *
   * @param newTupleSize
   *          the new tuple size
   * @return true, if check memory availability
   */
  private boolean checkMemoryAvailability(int newTupleSize) {

    if (startBufferingTimeInMemory == 0) {
      startBufferingTimeInMemory = System.currentTimeMillis();
    }
    if (tuplesCountInMemory == maxTuplesInMemory || tuplesSizeInMemory + newTupleSize > maxTuplesSizeInMemory
        || (System.currentTimeMillis() - startBufferingTimeInMemory) > maxBufferingTimeInMemory) {
      return false;
    }
    return true;
  }

  /**
   * Update disk state.
   */
  private void updateDiskState() {

    if (startBufferingTimeOnDisk == 0) {
      startBufferingTimeOnDisk = System.currentTimeMillis();
    }
    if (tuplesCountOnDisk > maxTuplesOnDisk || tuplesSizeOnDisk > maxBufferSizeOnDisk
        || (System.currentTimeMillis() - startBufferingTimeOnDisk) > maxBufferingTimeOnDisk) {
      saturated = true;
    }
  }

  /**
   * Clean.
   */
  public void clean() {
    tuples.clear();
    for (File file : files) {
      file.delete();
    }
    tuplesCountInMemory = 0;
    tuplesSizeInMemory = 0;
    startBufferingTimeInMemory = 0;
    tuplesCountOnDisk = 0;
    tuplesSizeOnDisk = 0;
    startBufferingTimeOnDisk = 0;

  }

  /**
   * Gets the files size.
   *
   * @return the files size
   */
  public int getFilesSize() {
    return files.size();
  }

  /**
   * Poll from disk.
   *
   * @return the file
   */
  public File pollFromDisk() {
    return files.poll();
  }

  /**
   * Checks if is saturated.
   *
   * @return true, if checks if is saturated
   */
  public boolean isSaturated() {
    return saturated;
  }

  /**
   * Gets the tuples count.
   *
   * @return the tuples count
   */
  public long getTuplesCount() {
    return tuplesCountInMemory + tuplesCountOnDisk;
  }

  @Override
  public Iterator<Tuple> iterator() {
    return tuples.iterator();
  }

}
