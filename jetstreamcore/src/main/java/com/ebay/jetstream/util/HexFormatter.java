/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util;


/**
 * Handle the byte / String conversion
 *
 * @author Ricky Ho
 *
 */
public class HexFormatter {

	/**
	 * Convert a byte to a string
	 *
	 * @param b
	 * @return
	 */
	public static String printHex(byte b) {
		int hd = (b & 0xF0) >>> 4, ld = b & 0x0F;
		return new String(new byte[] { (byte)(hd + (hd > 9 ? 'A' - 10 : '0')), (byte)(ld + (ld > 9 ? 'A' - 10 : '0')) });
	}

	public static char printAscii(byte b) {
		char c = (char)b;
		if (!Character.isDigit(c) && !Character.isLetter(c)) {
			c = '.';
		}
		return c;
	}

	public static String printHex(byte[] bArray) {
		StringBuilder result = new StringBuilder(bArray.length);
		for (byte b : bArray) {
			result.append(printHex(b));
		}
		return result.toString();
	}

	/**
	 * Print out a byte array in an easy to look format
	 *
	 * @param bArray
	 * @return
	 */
	private static String prettyPrintHex(byte[] bArray, int len, int maxSize, boolean isAscii) {
		StringBuilder result = new StringBuilder(len * 4 + (isAscii ? len * 3 : 0));
		StringBuilder asciiBuf = isAscii ? new StringBuilder(maxSize) : null;

		for (int i = 0; i < len; i++) {
			result.append(printHex(bArray[i])).append(' ');
			if (isAscii) {
				asciiBuf.append(printAscii(bArray[i]));
			}
			if (i % maxSize == maxSize - 1) {
				if (isAscii) {
					result.append('\t').append(asciiBuf);
					asciiBuf.setLength(0);
				}
				result.append('\n');
			}
		}

		if (isAscii){
			if(asciiBuf != null && asciiBuf.length() > 0){
				for (int i = asciiBuf.length(); i < maxSize; i++) {
					result.append("   ");
				}
				result.append('\t').append(asciiBuf);
			}
		}

		return result.toString();
	}

	public static String prettyPrintHex(byte[] bArray, int maxLineSize) {
		return prettyPrintHex(bArray, bArray.length, maxLineSize, true);
	}

	public static String prettyPrintHexWithoutAscii(byte[] bArray, int maxLineSize) {
		return prettyPrintHex(bArray, bArray.length, maxLineSize, false);
	}

	

}
