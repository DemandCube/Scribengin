package com.neverwinterdp.scribengin.yarn;

import java.util.LinkedList;
import java.util.List;

import com.beust.jcommander.JCommander;
import com.neverwinterdp.scribengin.scribeconsumer.ScribeConsumerConfig;


public class SampleAM extends AbstractApplicationMaster {

  public SampleAM() {
    super();
  }

  public static void main(String[] args) {
    System.out.println("ApplicationMaster::main"); //xxx
    AbstractApplicationMaster am = new SampleAM();
    new JCommander(am, args);
    am.init();

    try {
      am.run();
    } catch (Exception e) {
      System.out.println("am.run throws: " + e);
      e.printStackTrace();
      System.exit(0);
    }
  }

  @Override
  protected List<String> buildCommandList(ScribeConsumerConfig conf) {
    List<String> s = new LinkedList<String>();
    s.add("ls");
    return s;
  }

}
