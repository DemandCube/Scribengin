package com.neverwinterdp.scribengin.stream.sink;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.neverwinterdp.scribengin.stream.sink.partitioner.SinkPartitioner;
import com.neverwinterdp.scribengin.tuple.Tuple;

public class Buffer {

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

  private int tuplesSizeInMemory;

  /** The start time. */
  private long startBufferingTimeInMemory;

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

  private boolean diskBufferingEnabled;

  private boolean memoryBufferingEnabled;
  /** The logger. */
  private static Logger logger;
  /** The buffer. */
  private LinkedList<Tuple> tuples = new LinkedList<Tuple>();

  private int maxPurgingTimes;

  public Buffer(SinkPartitioner partitioner, S3SinkConfig config) {
    this.maxTuplesSizeInMemory = config.getMemoryMaxBufferSize();
    this.maxBufferingTimeInMemory = config.getMemoryMaxBufferingTime();
    this.maxTuplesOnDisk = config.getMemoryMaxTuples();
    this.maxBufferSizeOnDisk = config.getDiskMaxBufferSize();
    this.maxBufferingTimeOnDisk = config.getDiskMaxBufferingTime();
    this.maxTuplesInMemory = config.getMemoryMaxTuples();
    this.mappedByteBufferSize = config.getMappedByteBufferSize();
    this.partitioner = partitioner;
    this.chunkSize = config.getChunkSize();
    diskBufferingEnabled = config.isDiskBufferingEnabled();
    memoryBufferingEnabled = config.isMemoryBufferingEnabled();
    maxPurgingTimes = maxBufferSizeOnDisk / maxTuplesSizeInMemory;
    logger = LoggerFactory.getLogger("FileChannelBuffer");
  }

  public boolean add(Tuple tuple) {
    // if disk space is full return false
    if (maxPurgingTimes == 0) {
      return false;
    }
    if (memoryBufferingEnabled) {
      if (checkMemoryAvailability(tuple.getData().length)) {
        tuples.add(tuple);
        tuplesCountInMemory++;
        tuplesSizeInMemory += tuple.getData().length;
      }
      if (!checkMemoryAvailability(0)) {
        purgeMemoryToDisk();
        tuplesCountInMemory = 0;
      }
    } else {
      if (diskBufferingEnabled) {
        if (checkDiskAvailability(tuple.getData().length)) {
          return addOnDisk(tuple);
        } else {
          return false;
        }
      } else {
        return false;
      }

    }
    return true;
  }

  public boolean addOnDisk(Tuple tuple) {

    try {
      tuplesChunk.add(tuple);
      // write every chunk of tuples in one file
      if (tuplesChunk.size() == chunkSize) {
        long startOffset = tuplesChunk.getFirst().getCommitLogEntry().getStartOffset();
        long endOffset = tuplesChunk.getLast().getCommitLogEntry().getEndOffset();
        // call partitioner to get the path of the file depending of the
        // offset
        // the path will be later used to deduce the s3 path
        String path = partitioner.getPartition(startOffset, endOffset);
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
      }
      tuplesCountOnDisk++;

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return true;
  }

  /**
   * Buffer to disk.
   */
  private void purgeMemoryToDisk() {

    LinkedList<Tuple> tempTuples = tuples;
    tuples = new LinkedList<Tuple>();
    while (tempTuples.size() > 0) {
      addOnDisk(tempTuples.poll());
    }
    maxPurgingTimes--;

  }

  /**
   * Check state.
   * 
   * @param length
   */
  public boolean checkMemoryAvailability(int newTupleSize) {

    if (startBufferingTimeInMemory == 0) {
      startBufferingTimeInMemory = System.currentTimeMillis();
    }
    if (tuplesCountInMemory + 1 > maxTuplesInMemory || tuplesSizeInMemory + newTupleSize > maxTuplesSizeInMemory
        || (System.currentTimeMillis() - startBufferingTimeInMemory) > maxBufferingTimeInMemory
        || tuplesSizeInMemory + newTupleSize > (maxBufferSizeOnDisk - tuplesSizeOnDisk)) {
      return false;
    }
    return true;
  }

  public boolean checkDiskAvailability(int newTupleSize) {

    if (startBufferingTimeOnDisk == 0) {
      startBufferingTimeOnDisk = System.currentTimeMillis();
    }
    if (tuplesCountOnDisk + 1 > maxTuplesOnDisk || tuplesSizeOnDisk + newTupleSize > maxBufferSizeOnDisk
        || (System.currentTimeMillis() - startBufferingTimeOnDisk) > maxBufferingTimeOnDisk) {
      return false;
    }
    return true;
  }

  public void clean() {
    for (File file : files) {
      file.delete();
    }

  }

  public int getFilesSize() {
    return files.size();
  }

  public File pollFromDisk() {
    return files.poll();
  }

  public long getTuplesCount() {
    return tuplesCountInMemory + tuplesCountOnDisk;
  }

}
