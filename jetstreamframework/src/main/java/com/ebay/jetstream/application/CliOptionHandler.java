/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 * 
 */
package com.ebay.jetstream.application;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public interface CliOptionHandler {
  Options addOptions(Options options);

  void parseOptions(CommandLine line);
}