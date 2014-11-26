package com.neverwinterdp.scribengin.sink.s3;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;

public class Partitioner {
  private String topic;
  private int kafkaPartition;
  private long offset;
  private String bucketName;
  private String extension;

  // this one
  public Partitioner(String bucketName, String topic, int kafkaPartition, long offset, String extension) {
    this.bucketName = bucketName;
    this.topic = topic;
    this.kafkaPartition = kafkaPartition;
    this.offset = offset;
    this.extension = extension;

  }

  public String getLogFilePath() {
    int partition = (int) ((offset *1000)/1000);
    ArrayList<String> pathElements = new ArrayList<String>();
    pathElements.add(bucketName);
    pathElements.add(topic);
    pathElements.add("offset="+partition);

    return StringUtils.join(pathElements, "/");
  }

  public String getLogFileBasename() {
    ArrayList<String> basenameElements = new ArrayList<String>();
    basenameElements.add(Integer.toString(kafkaPartition));
    basenameElements.add(String.format("%020d", offset));
    return StringUtils.join(basenameElements, "_") + extension;
  }

  public void incOffset() {
    offset++;
  }

}
