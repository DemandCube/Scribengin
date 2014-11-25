package com.neverwinterdp.scribengin.stream.source;

import java.util.Random;
import java.util.UUID;

import com.neverwinterdp.scribengin.tuple.Tuple;

public class RandomFailureSourceStream implements SourceStream{

  private int failurePercentage;
  private UUIDSourceStream mSource;
  private String name;
  
  public RandomFailureSourceStream(){
    this(50);
  }
  
  public RandomFailureSourceStream(int failurePercentage){
    if(failurePercentage < 100 && failurePercentage > -1){
      this.failurePercentage = failurePercentage;
    }
    else{
      this.failurePercentage = 50;
    }
    this.mSource = new UUIDSourceStream();
    this.name = this.getClass().getSimpleName() +"-"+UUID.randomUUID().toString();
  }
  
  @Override
  public boolean prepareCommit() {
    if(this.decideToFail()){
      return false;
    }
    return this.mSource.prepareCommit();
  }

  @Override
  public boolean commit() {
    if(this.decideToFail()){
      return false;
    }
    return this.mSource.commit();
  }

  @Override
  public boolean clearCommit() {
    if(this.decideToFail()){
      return false;
    }
    return this.mSource.clearCommit();
  }

  @Override
  public boolean updateOffSet() {
    if(this.decideToFail()){
      return false;
    }
    return this.mSource.updateOffSet();
  }

  @Override
  public Tuple readNext() {
    return this.mSource.readNext();
  }

  @Override
  public boolean hasNext() {
    return this.decideToFail();
  }

  @Override
  public String getName() {
    return this.name;
  }
  
  private boolean decideToFail(){
    Random rand = new Random();
    if(this.failurePercentage > rand.nextInt(101)){
      return true;
    }
    return false;
  }

}
