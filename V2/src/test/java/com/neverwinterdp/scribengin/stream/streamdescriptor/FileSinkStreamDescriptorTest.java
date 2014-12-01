package com.neverwinterdp.scribengin.stream.streamdescriptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.neverwinterdp.scribengin.scribe.Scribe;
import com.neverwinterdp.scribengin.scribe.ScribeImpl;
import com.neverwinterdp.scribengin.stream.sink.FileSystemSinkStream;
import com.neverwinterdp.scribengin.stream.source.SequentialIntSourceStream;
import com.neverwinterdp.scribengin.task.CopyTask;

public class FileSinkStreamDescriptorTest {
  @Test
  public void testFileSinkStreamDescriptor() throws InterruptedException, IOException{
    int bufferLimit = 5000;
    String testDir = "./"+UUID.randomUUID().toString()+"/";
    System.err.println("Working Directory = "+System.getProperty("user.dir")+testDir);
    
    Scribe scribe = new ScribeImpl(new SequentialIntSourceStream(), 
        new FileSystemSinkStream(testDir+"tmp", testDir+"commit/"), 
        new FileSystemSinkStream(testDir+"/invalid/tmp", testDir+"/invalid/commit/"), 
        new CopyTask(bufferLimit));

    

    assertTrue(scribe.init());
    assertTrue(scribe.getTupleTracker().validateCounts());
    
    scribe.start();
    Thread.sleep(1500);
    scribe.stop();
    Thread.sleep(100);
    
    long firstScribeTupleCount = scribe.getTupleTracker().getWritten();
    
    //Create the second scribe from the descriptors of the first scribe
    Scribe secondScribe = new ScribeImpl(
        new SequentialIntSourceStream((OffsetStreamDescriptor)scribe.getSourceStream().getStreamDescriptor()), 
        new FileSystemSinkStream((FileSinkStreamDescriptor) scribe.getSinkStream().getStreamDescriptor()), 
        new FileSystemSinkStream((FileSinkStreamDescriptor) scribe.getInvalidSink().getStreamDescriptor()), 
        new CopyTask(bufferLimit));
    assertTrue(scribe.getTupleTracker().getWritten() > 0);
    assertTrue(scribe.getTupleTracker().validateCounts());
    
    assertTrue(secondScribe.init());
    assertTrue(secondScribe.getTupleTracker().validateCounts());
    
    secondScribe.start();
    Thread.sleep(1500);
    secondScribe.stop();
    Thread.sleep(100);
    
    assertEquals(firstScribeTupleCount, scribe.getTupleTracker().getWritten());
    assertTrue(secondScribe.getTupleTracker().getWritten() > 0);
    assertTrue(secondScribe.getTupleTracker().validateCounts());
    
    
    //Make sure there are no temp files and nothing committed to invalid sink
    File[] files = new File(testDir+"/invalid/commit/").listFiles();
    assertEquals(0, files.length);
    files = new File(testDir+"tmp").listFiles();
    assertNull(files);
    files = new File(testDir+"/invalid/tmp").listFiles();
    assertNull(files);
    
    //Make sure all the files are created correctly
    //Should be a sequential list of files listed numerically
    files = new File(testDir+"commit/").listFiles(); 
    Arrays.sort(files);
    LinkedList<Integer> listOfCommittedFiles = new LinkedList<Integer>();
    for(File f: files){
      if(!f.getName().startsWith(".")){
        listOfCommittedFiles.add(Integer.parseInt(f.getName()));
      }
    }
    Collections.sort(listOfCommittedFiles);
    
    //Make sure its written enough files
    assertTrue(listOfCommittedFiles.size() > 2);
    
    for(int i = 0; i < listOfCommittedFiles.size(); i++){
      //Make sure file exists
      assertEquals(new Integer(i), listOfCommittedFiles.get(i));
      
      if(i == listOfCommittedFiles.size() - 1){
        //We're at the last file and can't guarantee that this file has a full 5000 entries
        break;
      }
      
      //Make sure data is correct
      Path path = FileSystems.getDefault().getPath(testDir+"commit/", Integer.toString(i));
      String expectedData = "";
      for(int j = 1; j <= bufferLimit; j++){
        expectedData +=  Integer.toString((5000*i)+j);
      }
      
      assertEquals(expectedData, new String(Files.readAllBytes(path)));
      
    }
    
    
    FileUtils.deleteDirectory(new File(testDir));
  }
}
