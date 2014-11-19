package com.neverwinterdp.scribengin.source.kafka;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.log4j.Logger;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.neverwinterdp.scribengin.fixture.KafkaFixture;
import com.neverwinterdp.scribengin.fixture.ZookeeperFixture;
import com.neverwinterdp.scribengin.source.Source;
import com.neverwinterdp.scribengin.source.SourceDescriptor;
import com.neverwinterdp.scribengin.source.SourceStream;
import com.neverwinterdp.scribengin.source.SourceStreamDescriptor;
import com.neverwinterdp.scribengin.util.HostPort;
import com.neverwinterdp.scribengin.util.ZookeeperUtils;

// worry about reconnection, get partitions
public class KafkaSource implements Source {
  //TODO: implement kafka source and assign each kafka partition as a source stream
  //TODO KafkaSourceDescriptor  for topic metadata?
  //Source descriptor defines topic?

  private static final Logger logger = Logger.getLogger(KafkaSource.class);
  private static ZookeeperFixture zookeeperFixture;
  private static HashSet<KafkaFixture> kafkaBrokers;
  private static String topic = "scribe";
  private ZookeeperUtils utils;
  private SourceDescriptor descriptor;
  private Set<SourceStream> sourceStreams;

  public KafkaSource(SourceDescriptor descriptor) {
    super();
    this.descriptor = descriptor;
    sourceStreams = Sets.newHashSet();
    initialize();
  }

  //TODO agree if we need to cal this in constructor
  private void initialize() {
    sourceStreams = Sets.newHashSet(getSourceStreams());
  }

  @Override
  public SourceStream getSourceStream(int id) {
    // id is partition?
    // TODO how ensure sourceStreams is populated? call initialize()?
    return Iterables.getOnlyElement(Collections2.filter(sourceStreams, new IdPredicate(id)));
  }

  @Override
  public SourceStream getSourceStream(SourceStreamDescriptor descriptor) {
    // TODO how ensure sourceStreams is populated call initialize()?
    return Iterables.getOnlyElement(Collections2.filter(sourceStreams, new DescriptorPredicate(
        descriptor)));
  }

  //TODO add watcher
  @Override
  public SourceStream[] getSourceStreams() {
    logger.info("getSourceStreams. ");
    //kafka stores partitions as a String not an int
    //one partition, many brokers
    Multimap<String, HostPort> partitions = null;
    try {
      utils = new ZookeeperUtils(descriptor.getLocation());
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    }

    //get partitions for Topic
    try {
      partitions = utils.getBrokersForTopic(descriptor.getName());
      logger.info("NUMBER " + partitions);
    } catch (Exception e) {
    }
    //Create SourceStreams here
    KafkaSourceStreamDescriptor sourceStreamDescriptor;
    KafkaSourceStreamReader sourceStreamReader;
    KafkaSourceStream sourceStream;

    for (Entry<String, Collection<HostPort>> partition : partitions.asMap().entrySet()) {
      sourceStreamDescriptor =
          new KafkaSourceStreamDescriptor(descriptor.getName(), partition.getKey(),
              partition.getValue());
      sourceStream = new KafkaSourceStream(sourceStreamDescriptor);
      sourceStreamReader = new KafkaSourceStreamReader(descriptor.getName(), sourceStream);
      sourceStream.setSourceStreamReader(sourceStreamReader);
      sourceStreams.add(sourceStream);
    }
    return sourceStreams.toArray(new SourceStream[sourceStreams.size()]);
  }

  @Override
  public SourceDescriptor getSourceDescriptor() {
    return descriptor;
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    try {
      init();
      createKafkaData(100);
      SourceDescriptor descriptor = new SourceDescriptor();
      descriptor.setName(topic);
      descriptor.setLocation("127.0.0.1:2181");
      KafkaSource source = new KafkaSource(descriptor);

      Arrays.toString(source.getSourceStreams());
    } finally {
      stop();

    }
  }

  private static void stop() throws InterruptedException, IOException {
    Thread.sleep(1000);
    zookeeperFixture.stop();

    for (KafkaFixture kafkaFix : kafkaBrokers) {
      kafkaFix.stop();
    }

  }



  private static void init() throws IOException {
    zookeeperFixture = new ZookeeperFixture("0.8.1", "127.0.0.1", 2181);
    zookeeperFixture.start();

    kafkaBrokers = Sets.newHashSet();
    KafkaFixture kafkaFixture;
    for (int i = 0; i < 2; i++) {
      int kafkaPort = 9092;
      kafkaFixture = new KafkaFixture("0.8.1", "127.0.0.1", kafkaPort + i, "127.0.0.1", 2181);
      kafkaFixture.start();

      kafkaBrokers.add(kafkaFixture);
    }
  }

  private static void createKafkaData(int startNum) {
    Random rnd = new Random();
    logger.info("createKafkaData. " + startNum);
    long events = Long.parseLong(startNum + "");
    Properties props = new Properties();
    props.put("metadata.broker.list", "127.0.0.1:9092, 127.0.0.1:9093");
    props.put("num.partitions", Integer.toString(2));
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("partitioner.class", "com.neverwinterdp.scribengin.fixture.SimplePartitioner");
    props.put("request.required.acks", "1");

    ProducerConfig config = new ProducerConfig(props);

    Producer<String, String> producer = new Producer<String, String>(config);

    for (long nEvents = 0; nEvents < events; nEvents++) {
      long runtime = new Date().getTime();
      String ip = "192.168.2." + rnd.nextInt(255);
      String msg = runtime + ",www.example.com," + ip;
      KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, ip, msg);
      producer.send(data);
    }
    producer.close();
  }
}


class IdPredicate implements Predicate<SourceStream> {

  private int id;

  public IdPredicate(int id) {
    this.id = id;
  }

  @Override
  public boolean apply(SourceStream input) {
    return input.getDescriptor().getId() == id;
  }
}


class DescriptorPredicate implements Predicate<SourceStream> {

  private SourceStreamDescriptor descriptor;

  public DescriptorPredicate(SourceStreamDescriptor descriptor) {
    this.descriptor = descriptor;
  }

  @Override
  public boolean apply(SourceStream input) {
    return input.getDescriptor().equals(descriptor);
  }
}
