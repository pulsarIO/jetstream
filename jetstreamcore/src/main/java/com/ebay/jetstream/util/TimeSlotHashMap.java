/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 * 
 * 
 */
public class TimeSlotHashMap<T> extends TimerTask {

  public static class Key {
    public static Key newKey() {
      Key key = new Key();
      key.setGuid(GuidGenerator.gen());
      return key;
    }

    public static Key newKey(long id) {
      Key key = new Key();
      key.setGuid(id);
      return key;
    }

    public static Key newKey(String id) {
      Key key = new Key();
      StringTokenizer st = new StringTokenizer(id, "-");
      if (st.countTokens() != 2)
        return null;
      try {
        key.setTimeSlot(Integer.parseInt(st.nextToken()));
        key.setGuid(Long.parseLong(st.nextToken()));
      }
      catch (Throwable t) {
        return null;
      }

      return key;
    }

    private int m_timeSlot = 0;

    private long m_guid;

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

      if (!(obj instanceof Key))
        return false;

      Key key = (Key) obj;

      if (m_guid != key.m_guid)
        return false;
      if (m_timeSlot != key.m_timeSlot)
        return false;
      return true;
    }

    /**
     * @return the guid
     */
    public long getGuid() {
      return m_guid;
    }

    /**
     * @return the timeSlot
     */
    private int getTimeSlot() {
      return m_timeSlot;
    }

    /* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (m_guid ^ (m_guid >>> 32));
		result = prime * result + m_timeSlot;
		return result;
	}

	/**
     * @param guid
     *          the guid to set
     */
    public void setGuid(long guid) {
      m_guid = guid;
    }

    /**
     * @param timeSlot
     *          the timeSlot to set
     */
    private void setTimeSlot(int timeSlot) {
      m_timeSlot = timeSlot;
    }

    @Override
    public String toString() {
      StringBuffer buf = new StringBuffer();
      buf.append(getTimeSlot());
      buf.append("-");
      buf.append(getGuid());
      return buf.toString();
    }
  }

  public interface Listener {

    public void onTimeout(Object obj);
  }

  final static int NO_DELAY = 0;

  public static Key genKey() {
    return Key.newKey();
  }

  private int schedPeriod = 10000;
  private int m_numSlots = 6;
  private AtomicInteger m_slot = new AtomicInteger(0);
  private CopyOnWriteArrayList<ConcurrentHashMap<Key, T>> m_timeslotDB = new CopyOnWriteArrayList<ConcurrentHashMap<Key, T>>(); // key
  private final AtomicBoolean m_initialized = new AtomicBoolean(false);
  private Listener m_listener = null;

 
  
  public TimeSlotHashMap(Timer timer, int numTimeSlotsPerMinute, Listener listener) {
    
    m_numSlots = numTimeSlotsPerMinute;
    schedPeriod = 60000 / numTimeSlotsPerMinute;
    m_listener = listener;

    m_timeslotDB = new CopyOnWriteArrayList<ConcurrentHashMap<Key, T>>();

    for (int i = 0; i < m_numSlots; i++) {
      m_timeslotDB.add(new ConcurrentHashMap<Key, T>());
    }
    
    timer.scheduleAtFixedRate(this, NO_DELAY, schedPeriod);
    
    m_initialized.set(true);
    
  }

  /**
	 * 
	 */
  private void advance() {
    int prevSlot = m_slot.get();

    if (prevSlot == m_numSlots-1)
      m_slot.set(0);
    else
      m_slot.addAndGet(1);

    int gcSlot = 0;

    if (prevSlot == 0)
      gcSlot = m_numSlots - 1;
    else
      gcSlot = prevSlot - 1;

    gc(gcSlot);
  }

  /**
	 * 
	 */
  public void destroy() {
    cancel();
  }

  /**
   * @param slot
   */
  private void gc(int slot) {

    ConcurrentHashMap<Key, T> objCache = m_timeslotDB.get(slot);

    Set<Entry<Key, T>> objs = objCache.entrySet();

    Iterator<Entry<Key, T>> itr = objs.iterator();

    while (itr.hasNext()) {

      Entry<Key, T> entry = itr.next();

      Object obj = entry.getValue();

      if (m_listener != null)
        m_listener.onTimeout(obj);

    }

    objCache.clear();
  }

  /**
   * @param id
   * @return
   */
  public Object get(Key key) {

    return m_timeslotDB.get(key.getTimeSlot()).get(key);
  }

  /**
   * @return
   */
  public int getCurSlot() {
    return m_slot.get();
  }

  /**
   * @param id
   * @param obj
   */
  public void put(Key key, T object) {
    int curSlot = m_slot.get();
    key.setTimeSlot(curSlot);

    m_timeslotDB.get(curSlot).put(key, object);

  }

  /**
   * @param dispid
   */
  public T remove(Key key) {

    ConcurrentHashMap<Key, T> objCache = m_timeslotDB.get(key.getTimeSlot());

    T obj = objCache.remove(key);

    return obj;

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.util.TimerTask#run()
   */
  @Override
  public void run() {

   if (m_initialized.get())	  
	   advance();

  }

}
