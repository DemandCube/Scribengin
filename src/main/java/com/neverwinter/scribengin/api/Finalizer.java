package com.neverwinter.scribengin.api;

import org.apache.commons.chain.Filter;

// reader ------>converter ------>writer ------>Finalizer
// ^ ^ ^
// | | |
// filter buffer buffer
public interface Finalizer extends Filter {

  //update necessary system that we are done
  void finalizeit() throws Exception;


}
