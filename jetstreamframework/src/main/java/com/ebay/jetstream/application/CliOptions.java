/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 *
 */
package com.ebay.jetstream.application;

import java.io.PrintWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class CliOptions {
  private final CliOptionHandler m_optionHandler;
  private final Options m_options;

  public CliOptions(CliOptionHandler handler, String args[]) {
    m_optionHandler = handler;
    Options options = new Options();
    options.addOption("h", "help", false, "Help");
    m_options = m_optionHandler.addOptions(options);
    m_optionHandler.parseOptions(getCommandLine(args));
  }

  /**
   * Parse command line options and enforce rules, config file, app name and config version are mandatory
   * 
   * @param args
   */
  private CommandLine getCommandLine(String[] args) {
    CommandLine cl = null;
    try {
      cl = new GnuParser().parse(m_options, args);

      if (cl.hasOption('h')) {
        printUsageAndExit(true);
      }
    }
    catch (ParseException e) {
      printErrorAndExit(e.getMessage());
    }
    return cl;
  }

  private void printErrorAndExit(String message) {
    System.err.println("Error: " + message);
    printUsageAndExit(false);
  }

  private void printUsageAndExit(boolean verbose) {
    PrintWriter pw = new PrintWriter(System.err, true);
    HelpFormatter hf = new HelpFormatter();
    hf.printUsage(pw, 72, m_optionHandler.getClass().getCanonicalName(), m_options);
    if (verbose)
      hf.printOptions(pw, 72, m_options, 3, 3);
    System.exit(1);
  }
}