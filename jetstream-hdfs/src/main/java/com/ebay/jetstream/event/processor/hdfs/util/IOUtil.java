/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.processor.hdfs.util;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author weifang
 * 
 */
public class IOUtil {
	private static int BUFFER_SIZE = 1024;
	private static final Logger LOGGER = LoggerFactory.getLogger(IOUtil.class);

	public static void readAndWrite(InputStream in, OutputStream out)
			throws IOException {
		byte[] buffer = new byte[BUFFER_SIZE];
		int read;
		while (true) {
			read = in.read(buffer);
			if (read <= 0)
				break;
			out.write(buffer, 0, read);
		}
	}

	public static byte[] readBytes(InputStream in) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		readAndWrite(in, out);
		byte[] bytes = out.toByteArray();
		out.close();
		return bytes;
	}

	public static String readString(InputStream in) throws IOException {
		return new String(readBytes(in));
	}

	public static boolean delTree(String path) {
		File pathFile = new File(path);
		if (!pathFile.exists())
			return true;
		File[] files = pathFile.listFiles();
		for (int i = 0; i < files.length; i++) {
			if (files[i].isDirectory())
				delTree(files[i].getAbsolutePath());
			else {
				files[i].delete();
			}
		}
		return pathFile.delete();
	}

	public static void checkAndClose(Closeable closable) {
		checkAndClose(closable, LOGGER, null);
	}

	public static void checkAndClose(Closeable closable, Logger logger,
			String errorLog) {
		if (closable != null) {
			try {
				closable.close();
			} catch (Throwable e) {
				if (errorLog != null) {
					logger.error(errorLog + ". " + e.toString());
				}
			}
		}
	}

}