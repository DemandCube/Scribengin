package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.scribengin.scribe.Scribe;
import com.neverwinterdp.scribengin.streamcoordinator.StreamCoordinator;

/**
 *   DataFlow
 *     Scribe-1
 *       Stream
 *         source-stream
 *         sink-stream
 *         sink-invalid-stream
 *       Task
 *     Scribe-2
 *     
 *  1. I think I understand the stream in an other context, a stream of data suppose to come from data repository 
 *     such kafka with the same data type. If you organize the stream this way, I think the source stream and sink stream
 *     has the different data type and data repository.
 *     
 *  2. I saw you store the commit log in the tuple, as I understand you read each record from the source stream, 
 *     do you plan to call commit to create a new commit point for each read , if not do you think the commit log entry is
 *     the same with the previous one , until you call source commit to move to the new commit point.
 *     
 *  3. The idea is to have an reusable framework. What level do you plan to allow the developer custom his code and how. 
 *     Let's say that if you plan the developer just need to implement the Task interface to process the tuple. 
 *     it look simple, but how does the developer can control the source commit and sink commit. If you require the 
 *     developer to implement Scribe as well, then the developer has to call nextTuple as well? does is your intention?
 *     
 *  4. Remeber that the scribe-1, scribe-2 suppse to run in an isolation environment and in the different server. 
 *     The api look like to me that it won't satisfy this criteria.
 *  
 *  5. It look like to me that the scribe coordinator is responsible to split the source into stream?
 */
public interface Dataflow {
  //public DataflowConfig getDataflowConfig() ;

  //public void onModify(DataflowConfig config) ;
  
  public void setStreamCoordinator(StreamCoordinator s);
  
  public String getName();
  public void pause();
  public void stop();
  public void start();
  
  public void initScribes();
  public Scribe[] getScribes();
}

