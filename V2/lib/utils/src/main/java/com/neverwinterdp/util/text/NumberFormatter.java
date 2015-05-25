package com.neverwinterdp.util.text;

import java.text.DecimalFormat;

public class NumberFormatter {
  static DecimalFormat DECIMAL_FORMATER = new DecimalFormat("#.00");

  static public String byteFormatAsHumanReadable(long number) {
    if (number == 0) return "0";
    String suffix = "Bytes";
    String value = null;

    if (number > 1024 * 1024 * 1024l) {
      suffix = "GB";
      value = Double.toString(Math.round(number / (1024 * 1024 * 1024l)));
    } else if (number > 1024 * 1024l) {
      suffix = "MB";
      value = Double.toString(Math.round(number / (1024 * 1024l)));
    } else if (number > 1024l) {
      suffix = "KB";
      value = Double.toString(Math.round(number / 1024l));
    } else {
      value = Long.toString(number);
    }
    return value + "(" + suffix + ")";
  }

  static public String milliTimeAsHumanReadable(long number) {
    if (number > (60 * 60 * 1000l)) {
      return DECIMAL_FORMATER.format((double) number / (60 * 60 * 1000l)) + " h";
    } else if (number > 60 * 1000l) {
      return DECIMAL_FORMATER.format((double) number / (60 * 1000l)) + " m";
    } else if (number > 1000l) {
      return DECIMAL_FORMATER.format((double) number / 1000l) + " s";
    } else {
      return number + " ms";
    }
  }

  static public String nanoTimeAsHumanReadable(long number) {
    if (number > 1000000l) {
      long milli = number / 1000000l;
      return milliTimeAsHumanReadable(milli);
    } else {
      return number + " ns";
    }
  }
}
