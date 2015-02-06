/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 *
 */
package com.ebay.jetstream.management;

import java.util.EventListener;
import java.util.EventObject;

/**
 * 
 */
public interface ManagementListener extends EventListener {
  public static class BeanAddedEvent extends BeanChangedEvent {
    private static final long serialVersionUID = 1L;

    protected BeanAddedEvent(Object source, String path) {
      super(source, path);
    }
  }

  public static class BeanChangedEvent extends EventObject {
    private static final long serialVersionUID = 1L;

    private final String m_path;

    protected BeanChangedEvent(Object source, String path) {
      super(source);
      m_path = path;
    }

    public String getPath() {
      return m_path;
    }
  }

  public static class BeanRemovedEvent extends BeanChangedEvent {
    private static final long serialVersionUID = 1L;

    protected BeanRemovedEvent(Object source, String path) {
      super(source, path);
    }
  }

  void beanChanged(BeanChangedEvent event);
}
