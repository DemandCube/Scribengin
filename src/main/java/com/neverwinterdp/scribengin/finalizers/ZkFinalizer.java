package com.neverwinterdp.scribengin.finalizers;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.commons.chain.Chain;
import org.apache.commons.chain.Context;
import org.apache.log4j.Logger;

import com.neverwinterdp.scribengin.ScribenginContext;
import com.neverwinterdp.scribengin.utils.Progress;
import com.neverwinterdp.scribengin.utils.ScribenginUtils;

// UPdate zk on our progress
// TODO change to batch
public class ZkFinalizer implements Finalizer {

  private static final Logger logger = Logger.getLogger(ZkFinalizer.class);
  private ScribenginContext scribenginContext;
  private Properties props;

  //get offset from context
  //write to zk
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
    logger.info("Progress " + progress);
    scribenginContext.getZkHelper().updateProgress(zkProgressPath,
        ScribenginUtils.toJson(progress).getBytes());
  }

  private String getNow() {
    SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    sd.setTimeZone(TimeZone.getTimeZone("GMT"));
    return sd.format(new Date());
  }
}
