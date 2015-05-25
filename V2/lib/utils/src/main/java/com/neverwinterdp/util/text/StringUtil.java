package com.neverwinterdp.util.text;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
/**
 * @author Tuan Nguyen
 */
public class StringUtil {
  final static public  Charset UTF8 = Charset.forName("UTF-8") ;
  final static public  String[] EMPTY_ARRAY = {} ;
  final static public  String SEPARATOR = "," ; 
  
  
  static public boolean isEmpty(String s) {
    return s == null || s.length() == 0 ;
  }
  
  static public int compare(String s1, String s2) {
    if(s1 == null && s2 == null) return  0;
    if(s1 == null) return -1 ;
    if(s2 == null) return  1 ;
    return s1.compareTo(s2) ;
  }
  
  final static public String joinStringArray(String[] array) {
  	return joinStringArray(array, ",") ;
  }
  
  final static public String joinStringArray(String[] array, String separator) {
    if(array == null) return null ;
    if( array.length == 0) return "" ;
    StringBuilder b = new StringBuilder() ;
    for(int i = 0; i < array.length; i++) {
      if(array[i] == null) continue ;
      b.append(array[i]) ;
      if(i < array.length - 1) b.append(separator) ;
    }
    return b.toString() ;
  }
  
  
  
  final static public String join(Collection<?> collection, String separator) {
    if(collection == null) return null ;
    if(collection.size() == 0) return "" ;
    StringBuilder b = new StringBuilder() ;
    Iterator<?> i = collection.iterator() ;
    while(i.hasNext()) {
    	if(b.length() > 0) b.append(separator) ;
    	b.append(i.next()) ;
    }
    return b.toString() ;
  }

  final static public String join(Object[] array, String separator) {
    if(array == null) return null ;
    if( array.length == 0) return "" ;
    StringBuilder b = new StringBuilder() ;
    for(int i = 0; i < array.length; i++) {
      b.append(array[i]) ;
      if(i < array.length - 1) b.append(separator) ;
    }
    return b.toString() ;
  }
  
  final static public String joinIntArray(int[] array, String separator) {
    if(array == null) return null ;
    if( array.length == 0) return "" ;
    StringBuilder b = new StringBuilder() ;
    for(int i = 0; i < array.length; i++) {
      b.append(array[i]) ;
      if(i < array.length - 1) b.append(separator) ;
    }
    return b.toString() ;
  }
  
  final static public String joinArray(double[] array, String separator) {
    if(array == null) return null ;
    if( array.length == 0) return "" ;
    StringBuilder b = new StringBuilder() ;
    for(int i = 0; i < array.length; i++) {
      b.append(array[i]) ;
      if(i < array.length - 1) b.append(separator) ;
    }
    return b.toString() ;
  }
  
  final static public String[] join(String[] array1, String[] array2) {
    if(array1 == null && array2 == null) return null ;
    if(array1 == null) return array2 ;
    if(array2 == null) return array1 ;
    String[] newArray = new String[array1.length + array2.length] ;
    System.arraycopy(array1, 0, newArray, 0, array1.length) ;
    System.arraycopy(array2, 0, newArray, array1.length, array2.length) ;
    return newArray;
  }
  
  final static public String[] merge(String[] array1, String[] array2) {
    if(array1 == null && array2 == null) return null ;
    if(array1 == null) return array2 ;
    if(array2 == null) return array1 ;
    HashSet<String> set = new HashSet<String>();
    for (String string : array1) set.add(string);
    for (String string : array2) set.add(string);
    return toArray(set);
  }
  
  final static public String[] merge(String[] array, String string) {
    if(array == null && string == null) return null ;
    if(array == null) return new String[] { string } ;
    if(string == null) return array ;
    for(int i = 0; i < array.length; i++) {
      if(string.equals(array[i])) return array ;
    }
    String[] narray = new String[array.length + 1] ;
    for(int i = 0; i < array.length; i++) narray[i] = array[i] ;
    narray[array.length] = string ;
    return narray ;
  }
  
  final static public String[] removeDuplicate(String[] array) {
    if(array == null) return null ;
    HashSet<String> set = new HashSet<String>();
    for (String string : array) set.add(string);
    return toArray(set);
  }
  
  final static public String[] toStringArray(String s) {
    return toStringArray(s, SEPARATOR) ;
  }
  
  final static public String[] toStringArray(String s, String separator) {
  	if(s == null || s.length() == 0) return EMPTY_ARRAY ;
    String[] array = s.split(separator) ;
    for(int i = 0; i < array.length; i++) array[i] = array[i].trim() ;
    return array ;
  }
  
  final static public int[] toIntArray(String s, String separator) {
    if(s == null) return null ;
    String[] array = s.split(separator) ;
    for(int i = 0; i < array.length; i++) array[i] = array[i].trim() ;
    int[] value = new int[array.length] ;
    for(int i = 0; i < value.length; i++) value[i] = Integer.parseInt(array[i].trim()) ;
    return value ;
  }

  static public HashSet<String> toStringHashSet(String s) {
    return toStringHashSet(s, SEPARATOR) ;
  }
  
  static public HashSet<String> toStringHashSet(String s, String separator) {
    HashSet<String> set = new HashSet<String>() ;
    if(s == null || s.length() == 0) return set ;
    String[] array = s.split(separator) ;
    for(int i = 0; i < array.length; i++) set.add(array[i].trim()) ;
    return set ;
  }
  
  
  static public String[]  toArray(java.util.Collection<String> collection) {
    String[] array = new String[collection.size()] ;
    Iterator<String> i = collection.iterator() ;
    int index = 0 ;
    while(i.hasNext()) {
      array[index] = i.next() ;
      index++ ;
    }
    return array ;
  }
  
  static public String[]  toSortedArray(java.util.Collection<String> collection) {
    String[] array = toArray(collection) ;
    Arrays.sort(array);
    return array ;
  }
  
  static public List<String> toList(String[] array) {
    List<String> list = new ArrayList<String>() ;
    for(String s :  array) list.add(s) ;
    return list ;
  }
  
  static public void addList(List<String> holder, String ... array) {
    for(String s :  array) holder.add(s) ;
  }
  
  static public String eatCharacter(String string, char ignoreChar) {
    if(string == null || string.length() == 0) return string; 
    char[] array = string.toCharArray() ;
    StringBuilder b  = new StringBuilder() ;
    for(char c : array) {
      if(c != ignoreChar) b.append(c) ;
    }
    return b.toString() ;
  }
  
  final static public List<String> split(char[] buf, char separator) {
  	List<String> holder = new ArrayList<String>() ;
  	int idx = 0, start = 0 ;
    while(idx < buf.length) {
      if(buf[idx] == separator) {
        if(idx - start > 0) {
          holder.add(new String(buf, start, idx - start)) ;
        }
        idx++ ;
        start = idx ;
      } else {
        idx++ ;
      }
    }
    if(start < buf.length) {
      String s = new String(buf, start, buf.length - start) ;
      holder.add(s.trim()) ;
    }
    return holder;
  }
  
  final static public List<String> split(char[] buf, char[] separator) {
  	List<String> holder = new ArrayList<String>() ;
  	int idx = 0, start = 0 ;
    while(idx < buf.length) {
      if(isIn(buf[idx],separator)) {
        if(idx - start > 0) {
          holder.add(new String(buf, start, idx - start)) ;
        }
        idx++ ;
        start = idx ;
      } else {
        idx++ ;
      }
    }
    if(start < buf.length) {
      String s = new String(buf, start, buf.length - start) ;
      holder.add(s.trim()) ;
    }
    return holder;
  }
  
  final static public List<String> split(String string, char[] separator) {
  	return split(string.toCharArray(), separator) ;
  }
  
  final static public String[] splitAsArray(String string, char ... separator) {
  	List<String> holder = split(string.toCharArray(), separator) ;
  	return holder.toArray(new String[holder.size()]) ;
  }
  
  final static public List<String> split(String string, char separator) {
    char[] buf = string.toCharArray() ;
    return split(buf, separator) ;
  }
  
  final static public List<String> split(String string, String ... separator)  {
  	return split(string, separator, false) ;
  }
  
  final static public List<String> split(String string, String[] separator, boolean includeSeparator) {
    List<String> holder = new ArrayList<String>() ;
    final char[][] separatorBuf = new char[separator.length][] ;
    for(int i = 0; i < separator.length; i++) {
      separatorBuf[i] = separator[i].toCharArray() ;
    }
    
    char[] buf = string.toCharArray() ;
    int idx = 0, start = 0 ;
    while(idx < buf.length) {
      boolean match = false ;
      for(int j = 0; j < separatorBuf.length; j++) {
        match = matchAt(buf, idx, separatorBuf[j]) ; 
        if(match) {
          holder.add(new String(buf, start, idx - start)) ;
          if(includeSeparator) holder.add(separator[j]) ;
          idx += separatorBuf[j].length;
          start = idx ;
        }
      }
      if(!match) idx++ ;
    }
    if(start < buf.length) holder.add(new String(buf, start, buf.length - start)) ;
    return holder;
  }
  
  static public boolean isIn(String string, String[] set) {
    if(set == null || set.length == 0) return false ;
    if(string == null) return false ;
    for(String sel : set) if(string.equals(sel)) return true;
    return false ;
  }
  
  static public boolean isIn(String[] string, String[] set) {
    if(set == null || set.length == 0) return false ;
    if(string == null) return false ;
    if(string.length > set.length) return false ;
    for(String aString : string) {
    	boolean in = false ;
    	for(String sel : set) {
    		if(aString.equals(sel)) {
    			in = true ;
    			break ;
    		}
    	}
    	if(!in) return false ;
    }
    return true ;
  }
  
  static public boolean isContain(String string, String[] set, boolean flag) {
    if(set == null || set.length == 0) return false ;
    if(string == null) return false ;
    if (flag) {
      for(String sel : set) if(string.contains(sel)) return true;  
    } else {
      for(String sel : set) if(!string.contains(sel)) return true;
    }    
    return false ;
  }
  
  static public boolean isIn(char achar, char[] set) {
    for(char sel : set) if(achar ==  sel) return true;
    return false ;
  }
  
  final static boolean matchAt(char[] buf, int position, char[] bufToMatch) {
    if(position + bufToMatch.length > buf.length) return false ;
    for(int i = 0; i < bufToMatch.length; i++) {
      if(buf[position + i] != bufToMatch[i]) return false ;
    }
    return true ;
  }
  
  static public String[] removeStringWithPrefix(String[] string, String prefix) {
		if(string == null || string.length == 0) return string;
		boolean remove = false ;
		for(int i = 0; i < string.length; i++) {
			if(string[i].startsWith(prefix)) {
				string[i] = null ;
				remove = true ;
			}
		}
		if(!remove) return string ;
		String[] narray = new String[string.length - 1] ;
		int j = 0 ;
		for(int i = 0; i < string.length; i++) {
			if(string[i] != null) {
				narray[j++] = string[i] ;
			}
		}
		return narray ;
	}
}
