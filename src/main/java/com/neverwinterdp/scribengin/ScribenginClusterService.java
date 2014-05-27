package com.neverwinterdp.scribengin;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.neverwinterdp.queuengin.ReportMessageConsumerHandler;
import com.neverwinterdp.queuengin.kafka.KafkaMessageConsumerConnector;
import com.neverwinterdp.server.service.AbstractService;
/**
 * @author Tuan Nguyen
 * @email  tuan08@gmail.com
 */
public class ScribenginClusterService extends AbstractService {
  KafkaMessageConsumerConnector consumer ;
  
  @Inject(optional=true) @Named("kafka.zookeeper-urls")
  private String zookeeperUrls = "127.0.0.1:2181";
  
  private String[]   topic = {} ;

  @Inject
  public void setTopics(@Named("scribengin.consume-topics") String topics) {
    this.topic = topics.split(",") ;
  }
  
  public void start() throws Exception {
    String consumerGroup = "ScribenginClusterService" ;
    ReportMessageConsumerHandler handler = new ReportMessageConsumerHandler() ;
    consumer = new KafkaMessageConsumerConnector(consumerGroup, zookeeperUrls) ;
    for(String selTopic : topic) {
      consumer.consume(selTopic, handler, 1) ;
    }
  }

  public void stop() {
    consumer.close() ;
  }
}
