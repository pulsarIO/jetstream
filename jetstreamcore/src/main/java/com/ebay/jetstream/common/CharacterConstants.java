/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.common;

public class CharacterConstants {

	/* Line termination constants */
	public static final char CR_C = '\r';
	public static final byte CR_B = (byte)CR_C;
	public static final String CR_S = "\r";
	public static final char LF_C = '\n';
	public static final byte LF_B = (byte)LF_C;
	public static final String LF_S = "\n";
	public static final char[] CRLF_C = { CR_C, LF_C };
	public static final byte[] CRLF_B = { CR_B, LF_B };
	public static final String CRLF_S = "\r\n";

}
