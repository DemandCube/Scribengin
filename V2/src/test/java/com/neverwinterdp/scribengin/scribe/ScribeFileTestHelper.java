package com.neverwinterdp.scribengin.scribe;

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

public class ScribeFileTestHelper {
  public static void assertValidDataWrittenToFile(int bufferLimit, String testDir)
      throws IOException {
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
      //System.err.println("FILE: "+Integer.toString(i));
      //Make sure file exists
      assertEquals(new Integer(i), listOfCommittedFiles.get(i));
      
      //Make sure data is correct
      Path path = FileSystems.getDefault().getPath(testDir+"commit/", Integer.toString(i));
      String expectedData = "";
      for(int j = 1; j <= bufferLimit; j++){
        expectedData +=  Integer.toString((bufferLimit*i)+j)+"\n";
      }
      
      assertEquals(expectedData, new String(Files.readAllBytes(path)));
      
    }
  }
}
