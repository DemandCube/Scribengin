package com.neverwinterdp.util.text;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

/**
 * $Author: Tuan Nguyen$
 **/
public class TabularPrinter {
  private String     indent ;
  private int[]      width;
  private int        totalWidth;
  private Appendable out;

  protected TabularPrinter() {
  }

  public TabularPrinter(int[] width) {
    init(System.out, width);
  }

  public TabularPrinter(Appendable out, int[] width) {
    init(out, width);
  }

  public TabularPrinter(PrintStream out, int[] width) {
    init(out, width);
  }

  public TabularPrinter(OutputStream os, int[] width) throws UnsupportedEncodingException {
    init(new PrintStream(os, true, "UTF-8"), width);
  }

  protected void init(Appendable out, int[] width) {
    this.width = width;
    for (int sel : width)
      totalWidth += sel;
    this.out = out;
  }

  public TabularPrinter setIndent(String i) {
    this.indent = i ;
    return this ;
  }
  
  public void header(String ... header) {
    if(indent != null) print(indent) ;
    if (header.length != width.length) { throw new RuntimeException("Expect " + width.length + " column!!!"); }
    for (int i = 0; i < header.length; i++) {
      printCell(header[i], width[i]);
    }
    println();
    printRowSeparator('-');
  }

  public void row(Object ... cell) {
    row(cell, false);
  }

  public void row(Object[] cell, boolean printSeparator) {
    if(indent != null) print(indent) ;
    if (cell.length != width.length) { throw new RuntimeException("Expect " + width.length + " column!!!"); }
    for (int i = 0; i < cell.length; i++) {
      printCell(cell[i], width[i]);
    }
    println();
    if (printSeparator) printRowSeparator('-');
  }

  private void printCell(Object obj, int width) {
    String cell = "" ;
    if(obj != null) cell = obj.toString();
    int len = cell.length();
    if (len > width) {
      cell = cell.substring(0, width);
      len = cell.length();
    }
    print(cell);
    for (int i = len; i < width; i++) {
      print(" ");
    }
    print(" ");
  }

  private void printRowSeparator(char separator) {
    if(indent != null) print(indent) ;
    for (int i = 0; i < totalWidth; i++)
      print(separator);
    println();
  }

  private void print(String s) {
    try {
      out.append(s);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void print(char c) {
    try {
      out.append(c);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void println() {
    print('\n');
  }

  static public void main(String[] args) {
    int[] width = { 20, 20, 20 };
    String[] header = { "header 1", "header 2", "header 3" };
    String[] column = { "column 1", "column 2", "column 3" };
    TabularPrinter printer = new TabularPrinter(width);
    printer.setIndent("  ") ;
    printer.header(header);
    for (int i = 0; i < 10; i++) {
      printer.row(column, false);
    }
  }
}