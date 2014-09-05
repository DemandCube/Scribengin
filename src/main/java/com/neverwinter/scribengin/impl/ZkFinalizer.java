package com.neverwinter.scribengin.impl;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.commons.chain.Chain;
import org.apache.commons.chain.Context;
import org.apache.log4j.Logger;

import com.neverwinter.scribengin.api.Finalizer;
import com.neverwinter.scribengin.api.ScribenginContext;
import com.neverwinter.scribengin.utils.Utils;

// UPdate zk on our progress
// TODO change to batch
public class ZkFinalizer implements Finalizer {

  private static final Logger logger = Logger.getLogger(ZkFinalizer.class);
  private ScribenginContext scribenginContext;
  private Properties props;

  @Override
  public boolean execute(Context context) throws Exception {
    scribenginContext = (ScribenginContext) context;
    props = scribenginContext.getProps();
    finalizeit();
    return Chain.PROCESSING_COMPLETE;
  }

  @Override
  public boolean postprocess(Context context, Exception exception) {
    return false;
  }

  @Override
  public void finalizeit() throws Exception {
    logger.info("finalizeit.");
    String zkProgressPath = props.getProperty("zkProgressPath");

    Progress progress = new Progress();
    progress.setId(scribenginContext.getMemberId());
    progress.setOffset((Integer) scribenginContext.get(ScribenginContext.KAFKA_OFFSET));
    progress.setTimestamp(getNow());

    scribenginContext.getZkHelper().updateProgress(zkProgressPath,
        Utils.toJson(progress).getBytes());
  }

  private String getNow() {
    SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    sd.setTimeZone(TimeZone.getTimeZone("GMT"));
    return sd.format(new Date());
  }
}


class Progress {

  private int id;
  private int offset;
  private String timestamp;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public int getOffset() {
    return offset;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }
}
