/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.junit.Ignore;
import org.junit.Test;

public class FileUtilsTest {
	
	File file1 ;
	File  file2 ;
	
	@Test
	public void testappendContents() throws Exception{
		BufferedReader reader = null;
		StringBuffer all = new StringBuffer();
		try{
			file1 = File.createTempFile("123", "txt");
			System.out.println(file1.getAbsolutePath());
			System.out.println(file1.exists());
			FileUtils.appendContentsTofile(file1.getAbsolutePath(), "test");
			reader = new BufferedReader(new FileReader(file1));
			String oneline = null;
			while((oneline = reader.readLine()) != null){
				all.append(oneline);
			}
		}finally{
			if(reader != null)
				reader.close();
		}
			
		assertEquals("test", all.toString());
		
	}
	
	@Test
	public void testAppendSlash(){
		assertEquals("test/", FileUtils.appendSlash("test"));
		assertEquals("test/", FileUtils.appendSlash("test/"));
		assertEquals(null, FileUtils.appendSlash(null));
	}
	
	@Test
	public void testbcomp() throws Exception{
		file1 = File.createTempFile("111", "txt");
		FileUtils.appendContentsTofile(file1.getAbsolutePath(), "test");
		
		file2 = File.createTempFile("222", "txt");
		FileUtils.appendContentsTofile(file2.getAbsolutePath(), "test");
		
		assertTrue(FileUtils.bcmp(file1.getAbsolutePath(), file2.getAbsolutePath()));
		
		FileUtils.appendContentsTofile(file2.getAbsolutePath(), "onemoretest");
		assertFalse(FileUtils.bcmp(file1.getAbsolutePath(), file2.getAbsolutePath()));
		
	}
	
	@Ignore
	public void testcopyFile() throws Exception{
		
		file1 = File.createTempFile("test111", "txt");
		FileUtils.appendContentsTofile(file1.getAbsolutePath(), "test");
		
		file2 = File.createTempFile("test222", "txt");
		
		FileUtils.copyFile(file1.getAbsolutePath(), file2.getAbsolutePath());
		
		BufferedReader reader = new BufferedReader(new FileReader(file1));
		
		try{
			String oneline = null;
			StringBuffer all = new StringBuffer();
			while((oneline = reader.readLine()) != null){
				all.append(oneline);
			}
			System.out.println(all.toString());
		}finally{
			if(reader != null)
				reader.close();
		}
		
		BufferedReader reader1 = new BufferedReader(new FileReader(file2));
		try{
			String oneline1 = null;
			StringBuffer all1 = new StringBuffer();
			while((oneline1 = reader1.readLine()) != null){
				all1.append(oneline1);
			}
			System.out.println(all1.toString());
		}finally{
			if(reader1 != null)
				reader1.close();
		}
		
		assertTrue(FileUtils.bcmp(file1.getAbsolutePath(), file2.getAbsolutePath()));
		
	}
	
	

}
