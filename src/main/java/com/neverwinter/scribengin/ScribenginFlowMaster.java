package com.neverwinter.scribengin;

import java.util.concurrent.CyclicBarrier;

import org.apache.commons.chain.impl.ChainBase;
import org.apache.log4j.Logger;

import com.neverwinter.scribengin.buffers.ThreadBuffer;
import com.neverwinter.scribengin.converters.KafKaToHDFSConverter;
import com.neverwinter.scribengin.finalizers.ZkFinalizer;
import com.neverwinter.scribengin.readers.KafkaReader;
import com.neverwinter.scribengin.writers.HDFSWriter;

// Cluster aware
// bootstrap all reader, writer
// dynamic configuration via zk. store new data in context, commands will react accordingly.
// dynamic configuration - can be shut down remotely via zk.
// dynamic config - buffering can be modified on the fly.
// instance can be replaced remotely. hot swappable.

// It uses simple one-fail-all-fail error handling semantics. This means that any errors occurring
// in one or both services will result in an exception being thrown back to the invoker.
public class ScribenginFlowMaster extends ChainBase implements Runnable {

  //has to be a singleton
  private ScribenginContext context;
  private boolean isRunning;

  private static final Logger logger = Logger
      .getLogger(ScribenginFlowMaster.class);

  public ScribenginFlowMaster(ScribenginContext scribenginContext, int parties, int memberId) {
    super();
    logger.debug("SCR " + scribenginContext.getProps());
    addCommand(new KafkaReader());
    addCommand(new KafKaToHDFSConverter());
    addCommand(new ThreadBuffer(new CyclicBarrier(parties, new ThreadBuffer().new Updater())));
    addCommand(new HDFSWriter());
    addCommand(new ZkFinalizer());
    context = scribenginContext;
    context.setMemberId(memberId);
  }

  @Override
  public void run() {

    while (true) {
      if (isRunning()) {
        try {
          this.execute(context);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

  public ScribenginContext getContext() {
    return context;
  }

  public void setContext(ScribenginContext context) {
    this.context = context;
  }

  // TODO threading issues
  private boolean isRunning() {
    logger.debug("is Running " + isRunning);
    return isRunning;
  }

  public void setRunning(boolean isRunning) {
    this.isRunning = isRunning;
  }
}
