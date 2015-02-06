/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import com.ebay.jetstream.config.ConfigDataSource.ConfigStream;

//Configure dirPath before running this test case.
public class ConfigDataSourceTest {
  String startDirPath = "test" + File.separator + "junit";

  private String getLocation() {
    String osPath = resolvePathSeparatorForDotPattern(this.getClass().getPackage().getName());
    return startDirPath + File.separator + osPath + File.separator + "TestFolder";
  }

  private void loadConfigFiles(String location) {
    try {
      ConfigDataSource cds = ConfigUtils.getConfigDataSource(location);

      for (ConfigStream cs : cds) {
    	 InputStream is = null;
        try {
          is = cs.getStream();
          System.out.println(resolvePathSeparatorForSlashesPattern(cs.getLocation()));
          if(is !=null)
        	  printInputStream(is);
          System.out.println();
        }
        catch (FileNotFoundException fnfe) {
          if (!cs.getLocation().contains(".svn")) {
            System.out.println("FileNotFoundException Exception..."
                + resolvePathSeparatorForSlashesPattern(cs.getLocation() + " could be a folder..."));
            loadConfigFiles(cs.getLocation());
          }
        }finally{
        	try{
        		if(is!= null)
        			is.close();
        	}catch(IOException ioe){ } //ignore
        }
      } 
    }
    catch (Exception e) {
      Exception e1 = new RuntimeException(e);
      e1.printStackTrace();
    }
  }

  private void printInputStream(InputStream is) {
    try {
      int NOTHINGHASBEENREAD = 0;
      int originalSize = is.available();
      int bytesRead = NOTHINGHASBEENREAD;
      int stillToRead = originalSize;
      byte[] fileContents = new byte[originalSize];

      while (stillToRead > 0) {
        byte[] tempBuffer = new byte[originalSize];
        int actualRead = 0;
        actualRead = is.read(tempBuffer);
        System.arraycopy(tempBuffer, 0, fileContents, bytesRead, actualRead);
        bytesRead += actualRead;
        stillToRead = originalSize - bytesRead;
      }

      System.out.println(new String(fileContents));

    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }

  private String resolvePathSeparatorForDotPattern(String inputName) {
    String correctPath = inputName.replace(".", File.separator);
    return correctPath;
  }

  private String resolvePathSeparatorForSlashesPattern(String inputName) {
    String correctPath = inputName.replace("/", File.separator);
    correctPath = correctPath.replace("\\", File.separator);
    return correctPath;
  }

  // Test Method
  public void testWorkingOfConfigDataSource() {
    System.out.println("I still come in aww");
    loadConfigFiles(getLocation());
  }
}
