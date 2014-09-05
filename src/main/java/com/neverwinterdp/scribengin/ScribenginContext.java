package com.neverwinterdp.scribengin;

import java.util.Properties;

import org.apache.commons.chain.impl.ContextBase;

import com.neverwinterdp.scribengin.zookeeper.ZookeeperHelper;

// The data being passed round
public class ScribenginContext extends ContextBase {

  private static final long serialVersionUID = 1737809159685168939L;

  // reader to splitter
  // splitter to converter
  // converter to filter
  // filter to buffer
  // buffer to writer
  public static final String BYTE_BUFFER = "byteBuffer";
  public static final String CONVERTER_DATA = "converterData";
  public static final String BUFFER_DATA = "bufferData";
  public static final String WRITER_DATA = "writerData";
  public static final String KAFKA_OFFSET = "offset";

  // The reader will use this to determine offset
  // On success we use this to tell zookeeper our offset
  private ZookeeperHelper zkHelper;
  private String HDFSPath;
  private Properties props;
  private int memberId;


  public ZookeeperHelper getZkHelper() {
    return zkHelper;
  }

  public void setZkHelper(ZookeeperHelper zkHelper) {
    this.zkHelper = zkHelper;
  }

  public String getHDFSPath() {
    return HDFSPath;
  }

  public void setHDFSPath(String hDFSPath) {
    HDFSPath = hDFSPath;
  }

  public Properties getProps() {
    return props;
  }

  public void setProps(Properties props) {
    this.props = props;
  }

  public int getMemberId() {
    return memberId;
  }

  public void setMemberId(int memberId) {
    this.memberId = memberId;
  }
}
