package com.neverwinterdp.scribengin.s3.sink;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.neverwinterdp.scribengin.s3.sink.S3SinkConfig;
import com.neverwinterdp.scribengin.sink.partitioner.SinkPartitioner;
import com.neverwinterdp.scribengin.Record;

/**
 * The Class SinkBuffer.
 */
public final class S3SinkBuffer {

  private final long FIVE_GB = 5368709120l;

  /** The max tuples. */
  private long maxRecordsInMemory;

  /** The max buffer size. */
  private long maxRecordsSizeInMemory;

  /** The max buffering time. */
  private long maxBufferingTimeInMemory;

  /** The start time. */
  // TODO reset this after purge to memory
  private long startBufferingTimeInMemory;

  /** The files. */
  private LinkedList<String> files = new LinkedList<String>();

  /** The chunk size. */
  private int chunkSize;

  /** The partitioner. */
  private SinkPartitioner partitioner;

  /** The memory buffering enabled. */
  private boolean memoryBufferingEnabled;

  /** The logger. */
  private static Logger logger = LogManager.getLogger(S3SinkBuffer.class);
  /** The buffer. */
  private LinkedList<Record> tuples = new LinkedList<Record>();

  /** The local tmp dir. */
  private String localTmpDir;

  private Thread bufferThread;

  private boolean active = true;

  private long tuplesSizeInMemory;

  private LinkedList<Record> tuplesChunk = new LinkedList<Record>();

  /**
   * The Constructor.
   * 
   * @param partitioner
   *          the partitioner
   * @param config
   *          the configuration
   */
  public S3SinkBuffer(SinkPartitioner partitioner, S3SinkConfig config) {
    this.localTmpDir = config.getLocalTmpDir();
    this.maxRecordsSizeInMemory = config.getMemoryMaxBufferSize();
    this.maxBufferingTimeInMemory = config.getMemoryMaxBufferingTime();
    this.maxRecordsInMemory = config.getMemoryMaxRecords();
    // this.mappedByteBufferSize = config.getMappedByteBufferSize();
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

  }

  private void setProcessLoopActive(boolean active) {
    this.active = active;
  }

  /**
   * Adds the Record to the buffer.
   * 
   * @param tuple
   *          the tuple
   */
  public boolean add(Record tuple) {
    if (memoryBufferingEnabled) {
      if (!checkMemoryAvailability(tuple.getData().length)) {
        setProcessLoopActive(true);
        tuplesSizeInMemory = 0;
      }
      tuples.add(tuple);
      tuplesSizeInMemory += tuple.getData().length;
    } else {
      addToDisk(tuple);
    }
    return true;
  }

  /**
   * Adds the to disk.
   * 
   * @param tuple the tuple
   * @return true, if adds the to disk
   */
  private boolean addToDisk(Record tuple) {
    boolean success = false;
    RandomAccessFile randomAccessFile = null;
    FileChannel fileChannel = null;
    try {
      tuplesChunk.add(tuple);
      // write every chunk of tuples in one file

      if (tuplesChunk.size() == chunkSize) {

        try {
          // TODO retrieve the offset from the registry
          long startOffset = Long.parseLong(tuplesChunk.getFirst().getKey());
          long endOffset = Long.parseLong(tuplesChunk.getLast().getKey());
          // call partitioner to get the path of the file
          // depending on
          // the offset
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
          randomAccessFile = new RandomAccessFile(file, "rw");
          fileChannel = randomAccessFile.getChannel();
          MappedByteBuffer mem;
          for (Record t : tuplesChunk) {

            mem = fileChannel.map(FileChannel.MapMode.READ_WRITE, start, t.getData().length + 1);
            start += t.getData().length + 1;
            mem.put(t.getData());
            mem.put("\n".getBytes());
          }
          // add the file to the list of file created
          if (file.length() >= FIVE_GB) {
            throw new IllegalArgumentException("File created is bigger than allowed s3 sink file size.");
          }
          success = files.add(file.getCanonicalPath());
          tuplesChunk.clear();
        } catch (Exception e) {
          e.printStackTrace();
        }

        finally {

          randomAccessFile.close();
          fileChannel.close();
        }

      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return success;
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
   * Writes all tuples to disk creating more in-memory space. A few tuples that
   * couldn't complete a chunk will be left in-memory.
   */
  public void purgeMemoryToDisk() {
    logger.info("purge Memory To Disk");
    LinkedList<Record> tempTuples = tuples;
    tuples = new LinkedList<Record>();
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

    setProcessLoopActive(false);
  }

  /**
   * Check memory availability.
   * 
   * @param newRecordSize
   *          the new tuple size
   * @return true, if check memory availability
   */
  // TODO check space availability on disk as well?
  private boolean checkMemoryAvailability(int newRecordSize) {

    if (startBufferingTimeInMemory == 0) {
      startBufferingTimeInMemory = System.currentTimeMillis();
    }
    if (tuples.size() == maxRecordsInMemory || tuplesSizeInMemory + newRecordSize > maxRecordsSizeInMemory
        || (System.currentTimeMillis() - startBufferingTimeInMemory) > maxBufferingTimeInMemory) {
      return false;
    }
    return true;
  }

  /**
   * Clear tuples in memory and on disk.
   * 
   * @throws IOException
   */
  public void clear() throws IOException {
    String separator = System.getProperty("file.separator");
    File file = new File(localTmpDir + separator + partitioner.getPartition());
    try {
      FileUtils.deleteDirectory(file);

    } catch (Exception e) {
      e.printStackTrace();
    }
    tuples.clear();
    files.clear();
    startBufferingTimeInMemory = 0;
  }

  /**
   * Gets the files size.
   * 
   * @return the files size
   */
  public int getFilesCount() {
    return files.size();
  }

  /**
   * Poll from disk.
   * 
   * @return the file
   */
  // TODO name suggests that we actually read from disk?
  public File pollFromDisk() {
    return new File(files.poll());
  }

  /*
   * Note that there are other methods for reading on-File size vs in-Memory
   * sizes
   */
  public int size() {
    return tuples.size() + (files.size() * chunkSize);
  }

  public int tuplesInMemory() {
    return tuples.size();
  }

  public int tuplesOnDisk() {
    return (files.size() * chunkSize);
  }

  public boolean isEmpty() {
    return tuples.isEmpty() && files.isEmpty();
  }

  public boolean contains(Object o) {
    return tuples.contains(o);
  }

  // TODO also get file having tuple and remove
  public boolean remove(Object o) {
    return tuples.remove(o) && files.remove(o);
  }

  public boolean containsAll(Collection<?> collection) {

    return tuples.containsAll(collection);
  }

  public boolean addAll(Collection<? extends Record> ccollection) {
    boolean success = false;
    for (Record tuple : ccollection) {
      // TODO confirm if it does what it should
      success &= add(tuple);
    }
    return success;
  }

  // TODO and remove from files as well
  public boolean removeAll(Collection<?> collection) {
    return tuples.removeAll(collection);
  }

  /*
   * This methods exists solely for testing purposes.
   */
  public LinkedList<String> getFiles() {
    return files;

  }
}