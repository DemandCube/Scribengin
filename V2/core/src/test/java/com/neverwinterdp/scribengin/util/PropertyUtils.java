package com.neverwinterdp.scribengin.util;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.neverwinterdp.scribengin.storage.StreamDescriptor;

public class PropertyUtils {

    private static final Logger logger = LogManager.getLogger(PropertyUtils.class);
    private final String filename;
    private Properties properties;

    public PropertyUtils(String filename) {
        this.filename = filename;
        properties = getPropertyFile(filename);
    }

    public Properties getPropertyFile(String filename) {

        Properties prop = new Properties();
        InputStream inputStream = null;

        String path;
        try {
            path = System.getProperty("user.dir")
                    + System.getProperty("file.separator") + filename;
            logger.debug("Path " + path);
            inputStream = new FileInputStream(path);
        } catch (FileNotFoundException e) {
            try {
                URL urlpath = Thread.currentThread().getContextClassLoader()
                        .getResource(filename);
                inputStream = new FileInputStream(urlpath.getPath());
            } catch (FileNotFoundException e1) {
                e1.printStackTrace();
            }
        }
        try {
            if (inputStream != null) {
                prop.load(inputStream);
                inputStream.close();
            }
        } catch (IOException IOE) {
            IOE.printStackTrace();
        }
        return prop;
    }

    boolean saveProperty(String key, String value) {

        properties.setProperty(key, value);
        String path = System.getProperty("user.dir") + System.getProperty("file.separator") + filename;
        try {
            properties.store(new FileOutputStream(path), null);
        } catch (FileNotFoundException e) {
            logger.error(e, e);
        } catch (IOException e) {
            logger.error(e, e);
        }
        return false;
    }

    /**
     * Gets the properties.
     * 
     * @return the properties
     */
    public Properties getProperties() {
        return properties;
    }

  public StreamDescriptor getDescriptor() {
    StreamDescriptor descriptor = new StreamDescriptor();
    for (Object key : properties.keySet()){
      descriptor.put((String)key, (String) properties.get(key));
    }
    return descriptor;
  }
    

}