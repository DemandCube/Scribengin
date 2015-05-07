package com.neverwinterdp.scribengin.storage.s3;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class S3ObjectWriter {
  private S3Client s3Client;
  private String bucketName;
  private String key;
  ObjectMetadata metadata = new ObjectMetadata();
  private PipedOutputStream pipedOutput;
  private PipedInputStream pipedInput;
  private WriteThread writeThread;

  public S3ObjectWriter(S3Client s3Client, String bucketName, String key, ObjectMetadata metadata) throws IOException {
    this.s3Client = s3Client;
    this.bucketName = bucketName;
    this.key = key;
    this.metadata = metadata;
    pipedOutput = new PipedOutputStream();
    pipedInput = new PipedInputStream(pipedOutput);
    writeThread = new WriteThread();
    writeThread.start();
  }

  public ObjectMetadata getObjectMetadata() {
    return this.metadata;
  }

  public void write(byte[] data) throws IOException {
    pipedOutput.write(data);
  }

  public void waitAndClose(long timeout) throws IOException, InterruptedException {
    pipedOutput.close();
    if (!writeThread.waitForTermination(timeout)) {
      throw new IOException("The writer thread cannot upload all the data to S3 in " + timeout + "ms");
    }
    pipedInput.close();
  }

  public class WriteThread extends Thread {
    boolean running = false;

    public void run() {
      running = true;
      s3Client.getAmazonS3Client().putObject(new PutObjectRequest(bucketName, key, pipedInput, metadata));
      running = false;
      notifyTermination();
    }

    synchronized void notifyTermination() {
      notify();
    }

    synchronized boolean waitForTermination(long timeout) throws InterruptedException {
      wait(timeout);
      return !running;
    }
  }
}
