package com.neverwinter.scribengin.api;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.chain.Command;

// Send reader to two writers.
/*
 * /-------Converter----------writer1
 * /
 * Reader-------funnel-<
 * \
 * \------Converter-----------writer2
 */
// send out multiple outputs
// handle routing
/**alternatively we may have to do away with the splitter concept
 * Benefits
 * 1. Exception handling is done for free.
 * 2. Atomicity guaranteed. 
 * 
 */



public interface Splitter<T> extends Command {

  Collection<T> split(T t);

  // Event bus that fires messages to all registered consumers of this
  final class Router<T> {

    ExecutorService executorService = Executors.newFixedThreadPool(8);
    Collection<T> tees;

    // route the collection of T's to the correct next step. probably a
    // converter
    // how handle exception?
  }
}
