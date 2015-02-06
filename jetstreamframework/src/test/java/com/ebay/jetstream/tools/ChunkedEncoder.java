/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.tools;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;


public class ChunkedEncoder {

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("usage: ChunkedEncoder input output");
			System.exit(1);
		}

		FileReader in = null;
		FileWriter out = null;
		try {
			in = new FileReader(args[0]);
			out = new FileWriter(args[1]);

			int r = 0;
			char buf[] = new char[128];
			while ((r = in.read(buf)) > 0) {
				writeChunk(buf, r, out);
			}

			writeChunk(null, 0, out);
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}finally{
			try {
				if (out != null)
					out.close();
				if (in != null)
					in.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}

	private static void writeChunk(char buf[], int len, FileWriter out) throws Exception {
		out.write(Integer.toHexString(len));
		out.write(";\r\n");
		if (buf != null) {
			out.write(buf, 0, len);
			out.write("\r\n");
		}
	}

}
