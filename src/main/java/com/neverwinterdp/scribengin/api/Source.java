package com.neverwinterdp.scribengin.api;

public interface Source<T> {

  //This exists because we may change Readers 
  //read() to 
  
  T read(Source<T> source);

}
