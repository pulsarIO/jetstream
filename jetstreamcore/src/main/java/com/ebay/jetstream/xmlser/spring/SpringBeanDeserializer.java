/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.xmlser.spring;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.Resource;

import com.ebay.jetstream.xmlser.IXmlDeserializer;
import com.google.common.base.Charsets;

public class SpringBeanDeserializer implements IXmlDeserializer {
  public Map<String, Object> deserialize(final String content) {
    DefaultListableBeanFactory beanfactory = new XmlBeanFactory(new Resource() {
      public long contentLength() throws IOException {
        // TODO Auto-generated method stub
        return 0;
      }

      public Resource createRelative(String relativePath) throws IOException {
        throw notSupported();
      }

      public boolean exists() {
        return false;
      }

      public String getDescription() {
        return null;
      }

      public File getFile() throws IOException {
        throw notSupported();
      }

      public String getFilename() {
        return null;
      }

      public InputStream getInputStream() throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        sb.append("<!DOCTYPE beans PUBLIC \"-//SPRING//DTD BEAN 2.0//EN\" \"http://www.springframework.org/dtd/spring-beans-2.0.dtd\">");
        sb.append("<beans default-lazy-init=\"true\">");
        sb.append(content);
        sb.append("</beans>");
        return new ByteArrayInputStream(sb.toString().getBytes(Charsets.UTF_8));
      }

      public URI getURI() throws IOException {
        throw notSupported();
      }

      public URL getURL() throws IOException {
        throw notSupported();
      }

      public boolean isOpen() {
        return false;
      }

      public boolean isReadable() {
        return false;
      }

      public long lastModified() throws IOException {
        return -1;
      }

      private IOException notSupported() {
        return new IOException("not supported");
      }
    });

    Map<String, Object> result = new HashMap<String, Object>();
    for (String beanName : beanfactory.getBeanDefinitionNames()) {
      result.put(beanName, beanfactory.getBean(beanName));
    }

    return result;
  }
}
