/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 *
 */
package com.ebay.jetstream.util;

import java.net.InetAddress;
import java.util.StringTokenizer;

import com.ebay.jetstream.config.ConfigException;

public class NetMask {
  private final String m_map;
  private final byte m_baseAddr[];
  private final byte m_mask[];

  public NetMask(String map) throws ConfigException {
    int slash = map.indexOf("/");
    if (slash < 0) {
      throw new ConfigException("Invalid network specification, missing slash: " + map);
    }

    String addr = map.substring(0, slash);
    StringTokenizer toks = new StringTokenizer(addr, ".");
    int comps = toks.countTokens();
    if (comps != 4 && comps != 8) {
      throw new ConfigException("Wrong number of components for IP address: " + map);
    }

    m_map = map;
    m_baseAddr = new byte[comps];
    m_mask = new byte[comps];

    for (int c = 0; c < comps; c++) {
      m_baseAddr[c] = (byte) Integer.parseInt(toks.nextToken());
    }

    int mask = Integer.parseInt(map.substring(slash + 1));

    for (int c = 0; c < comps; c++) {
      if (mask > 8) {
        m_mask[c] = (byte) 0xff;
        mask = mask - 8;
      }
      else if (mask > 0) {
        int bit = 7;
        int m = 0;
        while (mask > 0) {
          m |= 1 << bit;
          bit -= 1;
          mask -= 1;
        }
        m_mask[c] = (byte) m;
      }
      else {
        m_mask[c] = 0;
      }
    }
  }

  public String getMask() {
    return m_map;
  }

  public boolean isWithinMask(InetAddress addr) {
    byte baddr[] = addr.getAddress();
    if (baddr.length != m_baseAddr.length) {
      // IPv4 and IPv6 not compatible
      return false;
    }

    for (int b = 0; b < m_baseAddr.length; b++) {
      int m1 = baddr[b] & m_mask[b];
      int m2 = m_baseAddr[b] & m_mask[b];
      if (m1 != m2) {
        return false;
      }
    }

    return true;
  }

  @Override
  public String toString() {
    return super.toString() + ": " + m_map;
  }
}