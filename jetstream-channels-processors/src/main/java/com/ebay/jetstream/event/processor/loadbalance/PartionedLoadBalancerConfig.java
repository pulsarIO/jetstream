/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/

package com.ebay.jetstream.event.processor.loadbalance;

/**
*
* Configuration holder class for PartitionedLoadBalancer
* 
* Sample Spring XML syntax
* 
* <bean id="PartitionedloadbalancerConfig"
		class="com.ebay.jetstream.event.processor.loadbalance.PartionedLoadBalancerConfig">
	   <property name="partitionKeys">
			<map>
				<entry>
					<key>
						<value>Person</value>
					</key>
					
						<list>
						   <value>age</value>
						   <value>gender</value>
						</list>
					
				</entry>
			</map>
    </property>
	</bean>
* 
* @author shmurthy@ebay.com
*
*/

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.xmlser.XSerializable;

public class PartionedLoadBalancerConfig extends AbstractNamedBean implements XSerializable {

	private Map<String, List<String>> m_partitionKeys = new ConcurrentHashMap<String, List<String>>();
	
	public PartionedLoadBalancerConfig() {}
	
	public Map<String, List<String>> getPartitionKeys() {
		return Collections.unmodifiableMap(m_partitionKeys);
	}
	
	public List<String> getPartitionFields(String eventType) {
		return m_partitionKeys.get(eventType);
	}

	public void setPartitionKeys(Map<String, List<String>> partitionKeys) {
		Set<Entry<String, List<String>>> partKeySet = partitionKeys.entrySet();
		
		Iterator<Entry<String, List<String>>> itr = partKeySet.iterator();
		m_partitionKeys.clear();
		
		while(itr.hasNext()) {
			Entry<String, List<String>> entry = itr.next();
			List<String> fieldList = new CopyOnWriteArrayList<String>();
			fieldList.addAll(entry.getValue());
			m_partitionKeys.put(entry.getKey(), fieldList);
		}
		
	}
	
	

}
