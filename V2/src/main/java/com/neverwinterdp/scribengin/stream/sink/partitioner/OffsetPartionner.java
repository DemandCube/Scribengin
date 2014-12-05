package com.neverwinterdp.scribengin.stream.sink.partitioner;

import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;

import com.neverwinterdp.scribengin.tuple.Tuple;

public class OffsetPartionner implements SinkPartitioner{

  
  private String topic;
  private int kafkaPartition;
  private String bucketName;
  private String extension;

  // this one
  public OffsetPartionner(String bucketName, String topic, int kafkaPartition, String extension) {
    this.bucketName = bucketName;
    this.topic = topic;
    this.kafkaPartition = kafkaPartition;
    this.extension = extension;

  }

  public String getLogFilePath(long offset) {
    int partition = (int) ((offset /1000)*1000);
    ArrayList<String> pathElements = new ArrayList<String>();
    pathElements.add(bucketName);
    pathElements.add(topic);
    pathElements.add("offset="+partition);

    return StringUtils.join(pathElements, "/");
  }

  public String getLogFileBasename(long offset) {
    ArrayList<String> basenameElements = new ArrayList<String>();
    basenameElements.add(Integer.toString(kafkaPartition));
    basenameElements.add(offset+"");
    return StringUtils.join(basenameElements, "_");
  }

  public String getextension() {
    return extension;
  }
  @Override
  public String getPartition() {
return "";
  }

  @Override
  public String getPartition(long offset) {
    return getLogFilePath(offset) + "/"+ getLogFileBasename(offset);
  }

}
