package com.neverwinterdp.scribengin.sink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.neverwinterdp.scribengin.commitlog.CommitLogEntry;
import com.neverwinterdp.scribengin.stream.sink.FileSystemSinkStream;
import com.neverwinterdp.scribengin.stream.sink.SinkStream;
import com.neverwinterdp.scribengin.tuple.Tuple;

public class FileSystemSinkStreamTest {
  @Test
  public void testFileSystemSinkStream() throws IOException{
    String testDir = "./"+UUID.randomUUID().toString()+"/";
    System.err.println("Working Directory = "+System.getProperty("user.dir")+testDir);
    SinkStream sink = new FileSystemSinkStream(testDir+"tmp", testDir+"commit/");
    
    int i=0;
    for(; i<10; i++){
      assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer.toString(i).getBytes(), new CommitLogEntry("key",i,i))));
    }
    assertEquals(10L, sink.getBufferSize());
    
    assertTrue(sink.prepareCommit());
    assertEquals(10L, sink.getBufferSize());
    assertTrue(sink.commit());
    assertEquals(10L, sink.getBufferSize());
    assertTrue(sink.completeCommit());
    assertEquals(0L, sink.getBufferSize());
    
    //Now we'll do a commit and roll it back
    LinkedList<Tuple> originalData = new LinkedList<Tuple>();
    originalData.addAll(((FileSystemSinkStream)sink).getBuffer());
    for(; i<20; i++){
      assertTrue(sink.bufferTuple(new Tuple(Integer.toString(i), Integer.toString(i).getBytes(), new CommitLogEntry("key", i, i))));
    }
    assertEquals(10L, sink.getBufferSize());
    
    assertTrue(sink.prepareCommit());
    assertEquals(10L, sink.getBufferSize());
    assertTrue(sink.commit());
    assertEquals(10L, sink.getBufferSize());
    assertTrue(sink.rollBack());
    assertEquals(0L, sink.getBufferSize());
    
    assertEquals(originalData.size(), ((FileSystemSinkStream)sink).getBuffer().size());
    for(int j=0; j < originalData.size(); j++){
      assertTrue(originalData.get(j).equals(((FileSystemSinkStream)sink).getBuffer().get(j)));
    }
    
    Path path = FileSystems.getDefault().getPath(testDir+"commit/", "0");
    assertEquals("0123456789", new String(Files.readAllBytes(path)));
    
    FileUtils.deleteDirectory(new File(testDir));
  }
}
