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

import com.neverwinterdp.scribengin.stream.sink.partitioner.SinkPartitioner;
import com.neverwinterdp.scribengin.tuple.Tuple;

/**
 * The Class FileChannelBuffer.
 */
public class FileChannelBuffer implements DiskBuffer {

  /** The buffer size. */
  private long bufferSize;
  
  /** The files. */
  private List<File> files = new ArrayList<File>();
  
  /** The chunk size. */
  private int chunkSize;
  
  /** The partititioner. */
  private SinkPartitioner partititioner;
  
  /** The tuples chunk. */
  LinkedList<Tuple> tuplesChunk = new LinkedList<>();
  
  /** The tuples count. */
  private int tuplesCount;
  
  /** The tuples size. */
  private int tuplesSize;
  
  /** The start time. */
  private long startTime;

  /**
   * The Constructor.
   *
   * @param partititioner the partititioner
   * @param bufferSize the buffer size
   * @param chunkSize the chunk size
   */
  public FileChannelBuffer(SinkPartitioner partititioner, long bufferSize, int chunkSize) {
    this.partititioner = partititioner;
    this.chunkSize = chunkSize;
    this.bufferSize = bufferSize;

  }

  /* (non-Javadoc)
   * @see com.neverwinterdp.scribengin.stream.sink.DiskBuffer#add(com.neverwinterdp.scribengin.tuple.Tuple)
   */
  @Override
  public void add(Tuple tuple) {
    try {
      if(startTime ==0){
        startTime = System.currentTimeMillis();
      }
      tuplesChunk.add(tuple);
      // write every chunk of tuples in one file
      if (tuplesChunk.size() == chunkSize) {
        long startOffset = tuplesChunk.getFirst().getCommitLogEntry().getStartOffset();
        long endOffset = tuplesChunk.getLast().getCommitLogEntry().getEndOffset();
        // call partitioner to get the path of the file depending of the offset
        // the path will be later used to deduce the s3 path
        String path = partititioner.getPartition(startOffset, endOffset);
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
      }
      tuplesCount++;
      
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  /* (non-Javadoc)
   * @see com.neverwinterdp.scribengin.stream.sink.DiskBuffer#getAllFiles()
   */
  @Override
  public List<File> getAllFiles() {
    return files;
  }


  /* (non-Javadoc)
   * @see com.neverwinterdp.scribengin.stream.sink.DiskBuffer#getTuplesCount()
   */
  @Override
  public int getTuplesCount() {
    return tuplesCount;
  }

  /* (non-Javadoc)
   * @see com.neverwinterdp.scribengin.stream.sink.DiskBuffer#getTuplesSize()
   */
  @Override
  public int getTuplesSize() {
    // TODO Auto-generated method stub
    return tuplesSize;
  }

  /* (non-Javadoc)
   * @see com.neverwinterdp.scribengin.stream.sink.DiskBuffer#getDuration()
   */
  @Override
  public long getDuration() {
    return System.currentTimeMillis() - startTime;
  }

  /* (non-Javadoc)
   * @see com.neverwinterdp.scribengin.stream.sink.DiskBuffer#clean()
   */
  @Override
  public void clean() {
    for(File file : files){
      file.delete();
    }
    
  }



}
