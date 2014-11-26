package com.neverwinterdp.scribengin.sink.s3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.sink.SinkStreamWriter;

public class S3SinkStreamWriterImpl implements SinkStreamWriter {
  private List<Record> tmpHolder = new ArrayList<Record>();
  AmazonS3 s3;
  Partitioner partitionner;
  public S3SinkStreamWriterImpl(AmazonS3 s3, Partitioner partitionner) {
    this.s3 = s3;
    this.partitionner = partitionner;
  }

  @Override
  public void append(Record record) throws Exception {
    tmpHolder.add(record);
    
  }

  @Override
  public void commit() throws Exception {
    String path = partitionner.getLogFilePath(); 
    String name =partitionner.getLogFileBasename();
    File file = File.createTempFile(name, ".txt");
    Writer writer = new OutputStreamWriter(new FileOutputStream(file));
    for(Record record:tmpHolder){
      file.deleteOnExit();
      writer.write(record.getData().toString());
      
      
    }
    
    System.out.println("Uploading a new object to S3 from a file\n");
    
    PutObjectRequest object = new PutObjectRequest(path, name, file);
    writer.close();
    try {
      s3.putObject(object);
    } catch (AmazonServiceException ase) {
      System.out.println("Caught an AmazonServiceException, which means your request made it "
          + "to Amazon S3, but was rejected with an error response for some reason.");
      System.out.println("Error Message:    " + ase.getMessage());
      System.out.println("HTTP Status Code: " + ase.getStatusCode());
      System.out.println("AWS Error Code:   " + ase.getErrorCode());
      System.out.println("Error Type:       " + ase.getErrorType());
      System.out.println("Request ID:       " + ase.getRequestId());
    } catch (AmazonClientException ace) {
      System.out.println("Caught an AmazonClientException, which means the client encountered "
          + "a serious internal problem while trying to communicate with S3, "
          + "such as not being able to access the network.");
      System.out.println("Error Message: " + ace.getMessage());
    }
    tmpHolder.clear();

   
  }

  /**
   * Creates a temporary file with text data to demonstrate uploading a file to
   * Amazon S3
   * @param bs 
   *
   * @return A newly created temporary file with text data.
   *
   * @throws IOException
   */
  private static File createSampleFile(byte[] bs) throws IOException {
    File file = File.createTempFile("aws-java-sdk-", ".txt");
    file.deleteOnExit();

    Writer writer = new OutputStreamWriter(new FileOutputStream(file));
    writer.write(bs.toString());
    writer.close();
    return file;
  }

  /**
   * Displays the contents of the specified input stream as text.
   *
   * @param input
   *          The input stream to display as text.
   *
   * @throws IOException
   */
  private static void displayTextInputStream(InputStream input) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(input));
    while (true) {
      String line = reader.readLine();
      if (line == null)
        break;

      System.out.println("    " + line);
    }
    System.out.println();
  }

  @Override
  public void close() throws Exception {
    if (tmpHolder.size() > 0)
      commit();

  }

  @Override
  public boolean verifyLastCommit() throws Exception {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean discard() throws Exception {
    // TODO Auto-generated method stub
    return false;
  }

}
