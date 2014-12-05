package com.neverwinterdp.scribengin.sink;
import java.io.File;
import java.util.concurrent.ConcurrentNavigableMap;

import org.mapdb.*;
public class Test {

  public static void main(String[] args) {
    // TODO Auto-generated method stub
    // configure and open database using builder pattern.
    // all options are available with code auto-completion.
    DB db = DBMaker.newFileDB(new File("testdb"))
               .closeOnJvmShutdown()
               .encryptionEnable("password")
               .make();

    // open existing an collection (or create new)
    ConcurrentNavigableMap<Integer,String> map = db.getTreeMap("collectionName");

    map.put(1, "one");
    map.put(2, "two");
    // map.keySet() is now [1,2]

    db.commit();  //persist changes into disk

    map.put(3, "three");
    // map.keySet() is now [1,2,3]
    db.rollback(); //revert recent changes
    // map.keySet() is now [1,2]

    db.close();

  }

}
