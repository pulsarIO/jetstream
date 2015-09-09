/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.processor.hdfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;

import com.ebay.jetstream.application.JetstreamApplication;
import com.ebay.jetstream.event.processor.hdfs.util.IOUtil;

/**
 * @author weifang
 * 
 */
public abstract class JetstreamTestApp {
	public static void startApp(String configFile, int port) throws Exception {
		System.setProperty("COS", "Dev");
		System.setProperty("spring.profiles.active", "ut");

		String home = System.getProperty("user.home");
		String jsHome = home + "/tmp/js";

		File dir = new File(jsHome).getAbsoluteFile();
		if (dir.exists()) {
			IOUtil.delTree(dir.getAbsolutePath());
		}
		String jsConfDir = jsHome + "/JetstreamConf/";
		new File(jsConfDir).mkdirs();
		String jsConf = jsConfDir + configFile;
		FileOutputStream fos = new FileOutputStream(jsConf);
		InputStream is = JetstreamTestApp.class.getClassLoader()
				.getResourceAsStream("JetstreamConf/" + configFile);
		IOUtil.readAndWrite(is, fos);
		fos.close();
		is.close();

		System.setProperty("com.ebay.jetstream.config", jsConfDir);
		JetstreamApplication.main(new String[] { "-p", String.valueOf(port) });
	}

	public static void stopApp() throws Exception {
		JetstreamApplication.getInstance().shutdown();
	}
}
