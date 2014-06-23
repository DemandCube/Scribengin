package com.neverwinterdp.scribengin;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.neverwinterdp.queuengin.MetricsConsumerHandler;
import com.neverwinterdp.queuengin.kafka.KafkaMessageConsumerConnector;
import com.neverwinterdp.server.service.AbstractService;
import com.neverwinterdp.util.monitor.ApplicationMonitor;
/**
 * @author Tuan Nguyen
 * @email  tuan08@gmail.com
 */
public class ScribenginClusterService extends AbstractService {
  KafkaMessageConsumerConnector consumer ;
  
  @Inject
  private ApplicationMonitor appMonitor ;
  
  @Inject(optional=true) @Named("zookeeper-urls")
  private String zookeeperUrls = "127.0.0.1:2181";
  
  private String[]   topic = {} ;

  @Inject
  public void setTopics(@Named("consume-topics") String topics) {
    this.topic = topics.split(",") ;
  }
  
  public void start() throws Exception {
    String consumerGroup = "ScribenginClusterService" ;
    MetricsConsumerHandler handler = new MetricsConsumerHandler("Scribengin", appMonitor) ;
    consumer = new KafkaMessageConsumerConnector(consumerGroup, zookeeperUrls) ;
    for(String selTopic : topic) {
      consumer.consume(selTopic, handler, 1) ;
    }
  }

  public void stop() {
    consumer.close() ;
  }
}
