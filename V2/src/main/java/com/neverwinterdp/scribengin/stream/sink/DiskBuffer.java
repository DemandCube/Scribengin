package com.neverwinterdp.scribengin.stream.sink;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.neverwinterdp.scribengin.stream.sink.partitioner.SinkPartitioner;
import com.neverwinterdp.scribengin.tuple.Tuple;

// TODO: Auto-generated Javadoc
/**
 * The Class FileChannelBuffer.
 */
public class DiskBuffer extends Buffer {

  /** The buffer size. */
  private long bufferSize;
  
  /** The files. */
  private LinkedList<File> files = new LinkedList<File>();
  
  /** The chunk size. */
  private int chunkSize;
  
  /** The partitioner. */
  
  private SinkPartitioner partitioner;
  
  /** The tuples chunk. */
  LinkedList<Tuple> tuplesChunk = new LinkedList<>();
  
  /** The tuples count. */
  private int tuplesCount;
  
  /** The tuples size. */
  private int tuplesSize;
  
  
  /** The logger. */
  private static Logger logger;
  
  /**
   * The Constructor.
   *
   * @param partitioner the partitioner
   * @param bufferSize the buffer size
   * @param chunkSize the number of tuples per file
   * @param maxDiskBufferSize the max disk buffer size
   * @param maxDiskBufferingTime the max disk buffering time
   * @param maxTuplesInDisk the max tuples number on disk
   */
  
  

  public DiskBuffer(SinkPartitioner partitioner,S3SinkConfig config) {
    super(config.getDiskMaxBufferSize(), config.getDiskMaxBufferingTime(), config.getDiskMaxTuples());
    this.bufferSize = 1024;
    this.partitioner = partitioner;
    this.chunkSize = config.getChunkSize();
    logger = LoggerFactory.getLogger("FileChannelBuffer");
  }

  /* (non-Javadoc)
   * @see com.neverwinterdp.scribengin.stream.sink.DiskBuffer#add(com.neverwinterdp.scribengin.tuple.Tuple)
   */
  @Override
  public void add(Tuple tuple) {
    state = State.Appending;
    try {
      
     
      tuplesChunk.add(tuple);
      // write every chunk of tuples in one file
      if (tuplesChunk.size() == chunkSize) {
        long startOffset = tuplesChunk.getFirst().getCommitLogEntry().getStartOffset();
        long endOffset = tuplesChunk.getLast().getCommitLogEntry().getEndOffset();
        // call partitioner to get the path of the file depending of the offset
        // the path will be later used to deduce the s3 path
        String path = partitioner.getPartition(startOffset, endOffset);
        //create file using the path
        File file = new File(path);
        File parent = file.getParentFile();
        if(!parent.exists() && !parent.mkdirs()){
            throw new IllegalStateException("Couldn't create dir: " + parent);
        }
        // write a memory mapped file
        int start = 0;
        FileChannel fc = new RandomAccessFile(file, "rw").getChannel();
        for (Tuple t : tuplesChunk) {
          MappedByteBuffer mem = fc.map(FileChannel.MapMode.READ_WRITE, 0, bufferSize);
          if (!mem.hasRemaining()) {
            start += mem.position();
            mem = fc.map(FileChannel.MapMode.READ_WRITE, start, t.getData().length);
          }
          mem.put(t.getData());
          tuplesSize+= t.getData().length;
        }
        // add the file to the liste of file created
        files.add(file);
        tuplesChunk.clear();
        updateState();
      }
      tuplesCount++;
      
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }


  /* (non-Javadoc)
   * @see com.neverwinterdp.scribengin.stream.sink.Buffer#getTuplesCount()
   */
  @Override
  public int getTuplesCount() {
    return tuplesCount;
  }

  /* (non-Javadoc)
   * @see com.neverwinterdp.scribengin.stream.sink.Buffer#getTuplesSize()
   */
  @Override
  public int getTuplesSize() {
    return tuplesSize;
  }

  /* (non-Javadoc)
   * @see com.neverwinterdp.scribengin.stream.sink.Buffer#clean()
   */
  @Override
  public void clean() {
    for(File file : files){
      file.delete();
    }
    
  }

  /**
   * Poll.
   *
   * @return the file
   */
  public File poll() {
    state = State.Purging;
    return files.poll();
  }
  
  public int getFilesSize() {
    return files.size();
  }
 
}
