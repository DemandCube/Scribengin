package com.neverwinterdp.es.log4j;

import java.io.IOException;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.elasticsearch.ElasticsearchException;

import com.neverwinterdp.buffer.chronicle.MultiSegmentQueue;
import com.neverwinterdp.buffer.chronicle.Segment;
import com.neverwinterdp.es.ESClient;
import com.neverwinterdp.es.ESObjectClient;
import com.neverwinterdp.util.text.StringUtil;

public class ElasticSearchAppender extends AppenderSkeleton {
  private String[] connect ;
  private String   indexName ;
  private String   queueBufferDir;
  private int      queueMaxSizePerSegment = 100000;
  private boolean  queueError = false ;
  private MultiSegmentQueue<Log4jRecord> queue ; 
  
  private DeamonThread forwardThread ;
  
  public void close() {
    if(forwardThread != null) {
      forwardThread.exit = true ;
      forwardThread.interrupt() ; 
    }
  }

  public void activateOptions() {
    System.out.println("ElasticSearchAppender: Start Activate Elasticsearch log4j appender");
    try {
      queue = new MultiSegmentQueue<Log4jRecord>(queueBufferDir, queueMaxSizePerSegment) ;
    } catch (Exception e) {
      queueError = true ;
      e.printStackTrace();
    }
    forwardThread = new DeamonThread(); 
    forwardThread.setDaemon(true);
    forwardThread.start() ;
    System.out.println("ElasticSearchAppender: Finish Activate Elasticsearch log4j appender");
  }

  public void setConnects(String connects) {
    this.connect = StringUtil.toStringArray(connects) ;
  }
  
  
  public void setIndexName(String indexName) {
    this.indexName = indexName ;
  }
 
  public void setQueueBufferDir(String queueBufferDir) { this.queueBufferDir = queueBufferDir; }

  public void setQueueMaxSizePerSegment(int queueMaxSizePerSegment) {
    this.queueMaxSizePerSegment = queueMaxSizePerSegment;
  }

  public boolean requiresLayout() { return false; }

  protected void append(LoggingEvent event) {
    if(queueError) return ;
    Log4jRecord record = new Log4jRecord(event) ;
    try {
      queue.writeObject(record) ;
    } catch (Exception e) {
      queueError = true ;
      e.printStackTrace();
    }
  }
  
  public class DeamonThread extends Thread {
    private ESObjectClient<Log4jRecord> esLog4jRecordClient ;
    private boolean elasticsearchError = false ;
    private boolean exit = false ;
    
    boolean init() {
      try {
        esLog4jRecordClient = new ESObjectClient<Log4jRecord>(new ESClient(connect), indexName, Log4jRecord.class) ;
        esLog4jRecordClient.getESClient().waitForConnected(60 * 60 * 60) ;
      } catch(Exception ex) {
        ex.printStackTrace();
        return false ;
      }

      try {
        if (!esLog4jRecordClient.isCreated()) {
          esLog4jRecordClient.createIndexWith(null, null);
        }
      } catch(Exception ex) {
        ex.printStackTrace();
      }
      return true ;
    }
    
    public void forward() {
      while(true) {
        try {
          if(elasticsearchError) {
            Thread.sleep(60 * 1000);
            elasticsearchError = false ;
          }
          Segment<Log4jRecord> segment = null ;
          while((segment = queue.nextReadSegment(15000)) != null) {
            segment.open();
            while(segment.hasNext()) {
              Log4jRecord record = segment.nextObject() ;
              esLog4jRecordClient.put(record, record.getId());
            }
            queue.commitReadSegment(segment);
          }
        } catch(ElasticsearchException ex) {
          elasticsearchError = true ;
        } catch (InterruptedException e) {
          return ;
        } catch(Exception ex) {
          ex.printStackTrace() ; 
          return ;
        }
      }
    }
    
    void shutdown() {
      esLog4jRecordClient.close() ;
      if(exit) {
        try {
          if(queue != null) queue.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    
    public void run() {
      if(!init()) return ;
      forward() ;
      shutdown() ;
    }
  }
}
