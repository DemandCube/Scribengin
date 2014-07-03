package com.neverwinterdp.scribengin;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;


public class ScribeCommitLog {
  private static final int NUM_CORRECT_RECORDS = 3;
  private static final Logger log =
    Logger.getLogger(ScribeCommitLog.class);

  private FileSystem fs;
  //private List recentLogEntryList;
  private ListIterator<ScribeLogEntry> recentLogIter;
  private Path path;

  public ScribeCommitLog(String uri) throws IOException
  {
    this(uri, false);
  }

  public ScribeCommitLog(String uri, boolean useLocalFS) throws IOException
  {
    Configuration conf;
    path = new Path(uri);

    if (useLocalFS == false) {
      // hdfs
      conf = new Configuration();
      conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
      conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
      fs = FileSystem.get(URI.create(uri), conf);
    } else {
      // local
      // It's unit test's responsiblility to set fs.
    }
  }

  public void record(long startOffset, long endOffset, String srcPath, String destPath)
      throws NoSuchAlgorithmException, IOException
  {
    FSDataOutputStream os;
    if (fs.exists(path)) {
      os = fs.append(path);
    } else {
      os = fs.create(path);
    }

    ScribeLogEntry entry = new ScribeLogEntry(startOffset, endOffset, srcPath, destPath);
    String jsonStr = ScribeLogEntry.toJson(entry);
    os.write(jsonStr.getBytes());
    os.write('\n');

    try {
      os.close();
    } catch (IOException e) {
      //TODO: log
    }
    fsClose();
  }

  public void read() throws IOException
  {
    ArrayList<ScribeLogEntry> logEntryList = new ArrayList<ScribeLogEntry>();

    if (fs.exists(path)) {
      FileStatus status = fs.getFileStatus(path);
      long fptr = status.getLen() - 1;

      FSDataInputStream in = fs.open(path);
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();


      int correctEntryCnt = 0;
      System.out.println(">>fptr : " + fptr); //xxx
      while (fptr >= 0) {
        in.seek(fptr);
        byte b = in.readByte();
        if (b != '\n') {
          buffer.write(b);
        }

        if (b == '\n' || fptr == 0) {
          if ( buffer.size() > 0 ) {
            // read from the back of the file, so we'll have to reverse the string.
            String jsonStr = new StringBuffer(buffer.toString()).reverse().toString();
            buffer.reset();
            //TODO: log the jsonStr's content
            System.out.println(">> jsonStr: " + jsonStr); //xxx

            ScribeLogEntry e = ScribeLogEntry.fromJson(jsonStr);

            logEntryList.add(e);

            try {
              if (e.isCheckSumValid()) {
                correctEntryCnt++;
              }
            } catch (NoSuchAlgorithmException ex) {
              //TODO: log
            }

            if (correctEntryCnt == NUM_CORRECT_RECORDS) {
              break;
            }
          }
        }
        fptr--;
      } //while

      // Check to see if the last character is '\n'
      // If not, make sure to write an extra '\n' to the log file
      in.seek(status.getLen() - 1);
      if (in.readByte() != '\n') {
         FSDataOutputStream os = fs.append(path);
         os.write('\n');
         try {
           os.close();
         } catch (IOException e) {
           //TODO: log
         }
      }

      fsClose();
    }

    Collections.reverse( logEntryList );
    recentLogIter = logEntryList.listIterator( logEntryList.size() );
  }

  public void readLastTwoEntries() throws IOException, NoSuchAlgorithmException
  {
    CircularFifoBuffer buffer = new CircularFifoBuffer(2);

    if (fs.exists(path)) {
      String currJsonStr;
      FSDataInputStream in = fs.open(path);

      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      while ((currJsonStr = br.readLine()) != null) {
        ScribeLogEntry e = ScribeLogEntry.fromJson(currJsonStr);
        buffer.add(e);
      }// while

      try{
        in.close();
      } catch(IOException e) {
        //TODO: log
      }
      fsClose();
    }

    List<ScribeLogEntry> recentLogEntryList = (List<ScribeLogEntry>)(List<?>)Arrays.asList(buffer.toArray());
    recentLogIter = recentLogEntryList.listIterator(recentLogEntryList.size());
  }

  // return null, if there's no more entry in the log
  public ScribeLogEntry getLatestEntry() {
    ScribeLogEntry r = null;
    if (recentLogIter.hasPrevious()) {
      r = recentLogIter.previous();
    }
    return r;
  }

  public void fsClose()
  {
    try {
      fs.close();
    } catch (IOException e) {
      // TODO: log
    }
  }
}
