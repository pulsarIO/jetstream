/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.support;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.ebay.jetstream.event.JetstreamEvent;

public class JetstreamEventList implements List<JetstreamEvent> {

  private final List<JetstreamEvent> m_backingList = new ArrayList<JetstreamEvent>();

  @Override
  public boolean add(JetstreamEvent object) {
    m_backingList.add(object);
    return false;
  }

  @Override
  public void add(int location, JetstreamEvent object) {
    m_backingList.add(location, object);
  }

  @Override
  public boolean addAll(Collection<? extends JetstreamEvent> collection) {

    return false;
  }

  @Override
  public boolean addAll(int location, Collection<? extends JetstreamEvent> collection) {

    return false;
  }

  @Override
  public void clear() {
    m_backingList.clear();

  }

  @Override
  public boolean contains(Object object) {
    return m_backingList.contains(object);
  }

  @Override
  public boolean containsAll(Collection<?> collection) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public JetstreamEvent get(int location) {
    return m_backingList.get(location);
  }

  @Override
  public int indexOf(Object object) {
    return m_backingList.indexOf(object);
  }

  @Override
  public boolean isEmpty() {
    return m_backingList.isEmpty();
  }

  @Override
  public Iterator<JetstreamEvent> iterator() {
    return m_backingList.iterator();
  }

  @Override
  public int lastIndexOf(Object object) {
    return m_backingList.lastIndexOf(object);
  }

  @Override
  public ListIterator<JetstreamEvent> listIterator() {
    return m_backingList.listIterator();
  }

  @Override
  public ListIterator<JetstreamEvent> listIterator(int location) {
    return m_backingList.listIterator(location);
  }

  @Override
  public JetstreamEvent remove(int location) {
    return m_backingList.remove(location);
  }

  @Override
  public boolean remove(Object object) {
    return m_backingList.remove(object);
  }

  @Override
  public boolean removeAll(Collection<?> collection) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean retainAll(Collection<?> collection) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public JetstreamEvent set(int location, JetstreamEvent object) {
    return m_backingList.set(location, object);
  }

  @Override
  public int size() {
    return m_backingList.size();
  }

  @Override
  public List<JetstreamEvent> subList(int start, int end) {
    return m_backingList.subList(start, end);

  }

  @Override
  public Object[] toArray() {
    return m_backingList.toArray();
  }

  @Override
  public <T> T[] toArray(T[] array) {
    return m_backingList.toArray(array);
  }

}
