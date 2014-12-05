package com.neverwinterdp.scribengin.sink.config;


import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;

public class Main {

  @Inject
  @Prop(Property.FOO)
  private String foo;

  @Inject
  AllProps all;

  public void run() {
    System.out.println("FOO: " + foo);
    System.out.println("All props:" + all);
  }

  public static void main(String[] args) {
    Injector injector = Guice.createInjector(new MyModule());
    injector.getInstance(Main.class).run();
  }
}
