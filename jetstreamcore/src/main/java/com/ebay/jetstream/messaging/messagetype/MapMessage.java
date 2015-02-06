/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.messagetype;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;


/**
 * @author shmurthy
 *
 * a message that accepts key value pairs which can be sent across the wire. it implements all
 * the marshalling code. The keys and values are java objects
 */
public class MapMessage extends JetstreamMessage implements Externalizable {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private HashMap<Object, Object> m_map = new HashMap<Object, Object>();
  
  
  public MapMessage() {}
  
  /* (non-Javadoc)
   * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
   */
  public void readExternal(ObjectInput in) throws IOException {
    
    super.readExternal(in);
    
    m_map = new HashMap<Object, Object>();
    
    int size = in.readInt();
    
    for (int i=0; i < size; i++) {
      try {
        m_map.put(in.readObject(), in.readObject());
      }
      catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
    }
    
  }
  
  
  /* (non-Javadoc)
   * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
   */
  public void writeExternal(ObjectOutput out) throws IOException {
    
    super.writeExternal(out);
    
    out.writeInt(m_map.size());
       
    Iterator<Entry<Object, Object>> itr = m_map.entrySet().iterator();
    
    while(itr.hasNext()) {
      
      Entry<Object, Object> entry = itr.next();
      
      out.writeObject(entry.getKey());
      out.writeObject(entry.getValue());
    }
  }
  
  /**
   * @return the map
   */
  public Map<Object, Object> getMap() {
    return m_map;
  }
  /**
   * @param map the map to set
   */
  public void setMap(HashMap<Object, Object> map) {
    m_map = map;
  }
  
  public void add(Object key, Object value) {
    m_map.put(key, value);
  }
  
  public Object get(Object key) {
    return m_map.get(key);
  }
  
  public void remove(Object key) {
    m_map.remove(key);
  }
  
}
