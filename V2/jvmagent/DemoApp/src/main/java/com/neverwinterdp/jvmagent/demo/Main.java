package com.neverwinterdp.jvmagent.demo;



public class Main {
  public static void main(String[] args) throws InterruptedException {
    System.out.println("DemoApp Hello");
    System.out.println("-------------");
    System.out.println("  Classloader = " + Thread.currentThread().getContextClassLoader().hashCode());
    Thread.sleep(5000);
  }
}