/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.RandomAccessFile;

/**
 * File utility class
 */

public class FileUtils {
  public static void appendContentsTofile(String fileName, String contents) throws Exception {
	  RandomAccessFile file = null;
	 try{
		    file = new RandomAccessFile(fileName, "rw");
		    file.seek(file.length());
		    file.writeBytes(contents);
	 }finally{
		 if(file != null)
			 file.close();
	 }
  }

  public static String appendSlash(String s) {
    if (s == null) {
      return null;
    }

    s = s.trim();

    if (s.endsWith("/")) {
      return s;
    }
    else {
      return s + "/";
    }
  }

  /** byte-by-byte file comparison */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="OBL_UNSATISFIED_OBLIGATION")
  public static boolean bcmp(String fn1, String fn2) {
	FileInputStream f1 = null;
	FileInputStream f2 = null;
	
    try {
      boolean result = true;
      f1 = new FileInputStream(fn1);
      f2 = new FileInputStream(fn2);
      int b1, b2, offset = 0;
      while (true) {
        b1 = f1.read();
        b2 = f2.read();
        offset++;

        if (b1 != b2) {
          result = false;
        }
        if (b1 < 0 && b2 < 0) { // Both finished
          break;
        }
        else if (b1 < 0 || b2 < 0) { // One finished
          result = false;
          break;
        }
      }
      return result;

    }
    catch (Throwable e) {
      
    }finally{
    	try{
    	if(f1 != null)
    		f1.close();
    	if(f2 != null)
    		f2.close();
    	}catch(IOException ioe){}
    	
    }

    return false;

  } // End bcmp

  public static boolean checkFileExists(String fileName) {
    File f = new File(fileName);
    return f.exists();
  }

  /**
   * copy method from From E.R. Harold's book "Java I/O"
   */
  public static void copy(InputStream in, OutputStream out) throws IOException {
    // do not allow other threads to read from the
    // input or write to the output while copying is
    // taking place
    synchronized (in) {
      synchronized (out) {

        byte[] buffer = new byte[256];
        while (true) {
          int bytesRead = in.read(buffer);
          if (bytesRead == -1)
            break;
          out.write(buffer, 0, bytesRead);
        }
      }
    }
  }

  /**
   * Copy a text file
   */
  public static void copyFile(String src, String dst) throws Exception {
    BufferedReader inFile = null;
    PrintWriter outFile = null;
    String line;

    // Read in & Write out
    try{
	    inFile = new BufferedReader(new FileReader(src));
	    outFile = new PrintWriter(new FileOutputStream(dst));
	
	    while ((line = inFile.readLine()) != null)
	      outFile.println(line);
    }finally{
    	if(inFile != null)
    		inFile.close();
        if(outFile != null)
        	outFile.close();
    }
    

  }

  /**
   * Copy a binary file
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="OBL_UNSATISFIED_OBLIGATION")
  public static void cp(String src, String dst) throws Exception {
	 FileInputStream srcfis = new FileInputStream(src);
	 FileOutputStream dstfos = new FileOutputStream(dst);
		try {
			cpStream(srcfis, dstfos);
		} finally {
			if (srcfis != null)
				srcfis.close();
			if (dstfos != null)
				dstfos.close();
		}
  }

  public static void createFileAndWriteContents(String fileName, String contents) throws Exception {
    BufferedWriter writer = new BufferedWriter(new FileWriter(new File(fileName)));
    try{
    	writer.write(contents);
    }finally{
    	if(writer != null)
    		writer.close();
    }
    
  }

  /**
   * Create a directory path
   * 
   * @param path
   *            the path to create
   * @param mustCreate
   *            true iff the path cannot exist before
   * @throws IOException
   */
  public static void createPath(String path, boolean mustCreate) throws IOException {
    File fpath = new File(path);
    if (fpath.exists() && mustCreate)
      throw new IOException(path + " already exists");
    else if (!fpath.mkdirs())
      throw new IOException("cannot create path: " + path);
  }

  /**
   * Create a hierarchy of directories (a path) in the designated temp dir. If the JVM terminates normally, these will
   * be cleaned up.
   * 
   * @param path
   * @return
   * @throws IOException
   */
  public static File createTempPath(String path) throws IOException {
    String sep = File.separator;
    File tempDir = new File(System.getProperty("java.io.tmpdir") + sep + path);
    if (!tempDir.exists())
      if (!tempDir.mkdirs()) {
        boolean status = tempDir.delete();
        throw new IOException("creation failed for dir " + tempDir + "status = " + status);
      }
    tempDir.deleteOnExit();
    return tempDir;
  }

  public static boolean dirExists(String fName) {
    boolean result = false;

    File file = new File(fName);
    if (file != null) {
      result = file.exists() && file.isDirectory();
    }

    return result;

  }

  public static boolean emptyDir(String dirName) {
    boolean result = false;

    File dir = new File(dirName);
    if (dir != null && dir.isDirectory()) {
      File[] files = dir.listFiles();
      result = files.length == 0 ? true : false;
    }

    return result;

  }

  /**
   * Test if a file exists or not
   */
  public static boolean fileExists(String fName) {
    boolean result = false;

    File file = new File(fName);
    if (file != null) {
      result = file.exists() && file.isFile();
    }

    return result;

  }

  public static byte[] fileToByteArray(String fname) {
    File file = new File(fname);
    FileInputStream fis = null;

    try {
      fis = new FileInputStream(fname);
    }
    catch (FileNotFoundException e) {
      
      return null;
    }
   

    byte[] fileContents = new byte[(int) file.length()];

    int i = 0;

    while (true) {
      int data;
      try {
        data = fis.read();
      }
      catch (IOException e) {
              
        return null;
      }
      finally {
    	  if (fis != null)
			try {
				fis.close();
			} catch (IOException e) {
				//ignore
			}
      }

      if (data == -1)
        break;

      fileContents[i] = (byte) data;
    }
    return fileContents;
  }

  public static String readFile(String fileName, String newLine) throws IOException {
	  FileInputStream fis = new FileInputStream(fileName);
	  try{
		  return CommonUtils.getStreamAsString(fis, newLine);
	  }finally{
		  if(fis != null)
			  fis.close();
	  }
  }

  public static String readFileOrNull(String fileName) {
    try {
      return readFile(fileName, null);
    }
    catch (IOException e) {
    }
    return null;
  }

  public static void writeLine(BufferedWriter out, String line) throws Exception {
    out.write(line, 0, line.length());
    out.newLine();
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="REC_CATCH_EXCEPTION")
  public static boolean writeLine(String outFname, String line, boolean append) {
		BufferedWriter out = null;
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(outFname, append);
			out = new BufferedWriter(new OutputStreamWriter(fos, "UTF8"));
			out.write(line, 0, line.length());
			out.newLine();
			return true;
		} catch (Exception e) {
			return false;
		} finally {
			try {
				if (out != null)
					out.close();
				if (fos != null)
					fos.close();
			} catch (IOException e) {
			}
		}
  }

  /**
   * @param src
   *            source file or directory
   * @param dst
   *            destination file or directory
   */
  public static void xcopy(String src, String dst) throws Exception {
    File s = new File(src), d = new File(dst);

    if (s.isDirectory()) {
      if (!d.mkdirs()) return;
      String files[] = s.list();

      for (int i = 0; i < files.length; i++) {
        xcopy(src + '/' + files[i], dst + '/' + files[i]);
      }
    }
    else if (s.isFile())
      cp(src, dst);

  }

  private static void cpStream(FileInputStream fis, FileOutputStream fos) throws IOException {
    byte[] buf = new byte[512];
    int len;
    BufferedInputStream bis = new BufferedInputStream(fis);
    BufferedOutputStream bos = new BufferedOutputStream(fos);
    while ((len = bis.read(buf)) != -1)
      bos.write(buf, 0, len);
    bos.flush();
  }

  private FileUtils() {
  }
}
