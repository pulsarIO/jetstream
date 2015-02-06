/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.topic;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version
 */

public class JetstreamTopic implements Externalizable {

  private static final long serialVersionUID = 1L;

  private String m_name;
  private ArrayList<String> m_contextList = null;

  /**
	 * 
	 */
  public JetstreamTopic() {
  }

  /**
   * @param name
   */
  public JetstreamTopic(String name) {
    m_name = name;
    parse();

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {

    if (obj == this)
      return true;

    if (obj == null)
      return false;

    if (!(obj instanceof JetstreamTopic))
      return false;

    JetstreamTopic topic = (JetstreamTopic) obj;

    if (!m_name.equals(topic.m_name))
      return false;

    return true;
  }

  /**
   * The contexts are ordered in the returned array such that the first context is in location 0 in the array and the
   * leaf is the last element of the array.
   */

  public Object[] getContexts() {
    if (m_contextList == null)
      m_contextList = new ArrayList<String>(10);

    return m_contextList.toArray();
  }

  /**
   * 
   * The following methods have specifically not been synchronized for performance We might want to consider
   * synchronizing if we see a need. Since the parsing happens when the constructor fires or setName() gets called, I am
   * willing to take the risk of not synchronizing the following 2 methods
   * 
   */

  public String getRootContext() {

    if (m_contextList == null)
      return null;

    return m_contextList.get(0);
  }

  /**
   * @return
   */
  public String getTopicName() {
    return m_name;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {

    return m_name.hashCode();

  }

  /**
   * This method is intended to be called on the sender's topic instance and the topic instance passed in to this method
   * must be the listener's topic instance
   * 
   * @param listenerTopic
   * @return
   */
  public boolean matches(JetstreamTopic listenerTopic) {
    if (listenerTopic.getRootContext().equals("/"))
      return true;

    Object[] passedContextList = listenerTopic.getContexts();

    // our rule is sender should always have equal or more contexts than the listener
    if (numContexts() < listenerTopic.numContexts())
      return false;

    Iterator<String> itr = m_contextList.iterator();

    int i = 0;

    while (itr.hasNext()) {

      if (!itr.next().equals(passedContextList[i++]))
        return false;

      if (i == passedContextList.length)
        break;
    }

    return true;
  }

  /**
   * @return
   */
  public int numContexts() {

    if (m_contextList != null)
      return m_contextList.size();

    return 0;
  }

  /**
	 * 
	 */
  private void parse() {

    if (m_name.equals("/")) {
      if (m_contextList == null)
        m_contextList = new ArrayList<String>(10);
      else
    	m_contextList.clear();
      m_contextList.add(m_name);
      return;
    }

    int previndex = 0;

    int nextindex = m_name.indexOf('/');

    if (nextindex < 0) {
      m_contextList = new ArrayList<String>(10);
      m_contextList.add(m_name);
      return; // no context specified

    }

    if (m_contextList == null)
      m_contextList = new ArrayList<String>(10);
    else
    	m_contextList.clear();

    // we should not have a context name greater than 100 characters

    char[] contextname = new char[100];

    Arrays.fill(contextname, (char) 0);

    m_name.getChars(previndex, nextindex, contextname, 0);

    m_contextList.add(new String(contextname, 0, nextindex - previndex));

    while (true) {

      previndex = nextindex + 1;

      nextindex = m_name.indexOf('/', previndex);

      if (nextindex == previndex)
        return;

      Arrays.fill(contextname, (char) 0);

      if (nextindex < 0) {
        nextindex = m_name.length();

        Arrays.fill(contextname, (char) 0);

        m_name.getChars(previndex, nextindex, contextname, 0);

        m_contextList.add(new String(contextname, 0, nextindex - previndex));

        return;
      }

      Arrays.fill(contextname, (char) 0);

      m_name.getChars(previndex, nextindex, contextname, 0);

      m_contextList.add(new String(contextname, 0, nextindex - previndex));

    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
   */
  public void readExternal(ObjectInput in) throws IOException {

    try {
      int nameLen = in.readInt();
      byte[] b = new byte[nameLen];
      in.readFully(b);
      m_name = new String(b);

      parse();
    }
    catch (IOException ioe) {
      throw ioe;
    }

  }

  /**
   * @param name
   */
  public void setTopicName(String name) {
    m_name = name;
    parse();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return m_name;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
   */
  public void writeExternal(ObjectOutput out) throws IOException {

    // out.writeObject(this.m_name);
    out.writeInt(m_name.length());
    out.write(m_name.getBytes());

  }

    public void writeKryo(Kryo kryo, Output output) {
        output.writeString(m_name);

    }

    public void readKryo(Kryo kryo, Input input) {
        m_name = input.readString();
        parse();
    }
}