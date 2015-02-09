package com.neverwinterdp.vm.client.shell;

import java.io.IOException;


public class Console {
   protected Appendable out ;
  
  public Console() {
    this.out = System.out ;
  }
  
  public Console(Appendable out) {
    this.out = out ;
  }
  
  public void print(String line) throws IOException {
    out.append(line);
  }
  
  public void println(String line) throws IOException {
    out.append(line).append('\n');
  }
  
  public void h1(String title) throws IOException {
    out.append(title).append('\n');
    String underline = title.replaceAll(".", "*");
    out.append(underline).append("\n");
  }
}
