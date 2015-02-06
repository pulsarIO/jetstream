/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.registry;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.messaging.topic.JetstreamTopic;
import com.ebay.jetstream.messaging.transport.netty.eventproducer.EventConsumerInfo;
import com.ebay.jetstream.messaging.transport.netty.schedulingalgorithm.lb.Selection;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 * 
 *          This registry holfs information about topic to consumer bindings - designed mainly to help weigthed algorithms.
 * 
 */

public class EventTopicRegistry implements XSerializable, Externalizable {

	public static class SelectionList implements Externalizable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private ArrayList<LinkedList<Selection>> m_weightedSelectionList = new ArrayList<LinkedList<Selection>>(); // each
																													// entry
																													// corresponds
																													// to
																													// a
																													// weight
		private WeightRegistry m_weightRegistry = new WeightRegistry();

		private int m_slcapacity = 11;

		/**
		 * 
		 */
		public SelectionList() {
		}

		/**
		 * @param capacity
		 */
		public SelectionList(int capacity) {
			m_slcapacity = capacity;
			m_weightedSelectionList = new ArrayList<LinkedList<Selection>>(
					m_slcapacity);

			for (int i = 0; i < m_slcapacity; i++) {
				m_weightedSelectionList.add(new LinkedList<Selection>());
			}
		}

		/**
		 * @param sel
		 * @param weight
		 */
		public void add(Selection sel, int weight) {
			LinkedList<Selection> selectionList = m_weightedSelectionList
					.get(weight);

			if (!selectionList.contains(sel))
				selectionList.add(sel);

			m_weightRegistry.add(weight);

		}

		/**
		 * @return
		 */
		public ArrayList<LinkedList<Selection>> getWeightedSelectionList() {
			return m_weightedSelectionList;
		}

		/**
		 * @return the weightRegistry
		 */
		public WeightRegistry getWeightRegistry() {
			return m_weightRegistry;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
		 */
		@SuppressWarnings("unchecked")
		public void readExternal(ObjectInput in) throws IOException,
				ClassNotFoundException {

			m_weightRegistry = (WeightRegistry) in.readObject();
			m_weightedSelectionList = (ArrayList<LinkedList<Selection>>) in
					.readObject();

		}

		/**
		 * @param sel
		 */
		public void remove(Selection sel) {

			Iterator<LinkedList<Selection>> weightListItr = m_weightedSelectionList
					.iterator();

			while (weightListItr.hasNext()) {
				LinkedList<Selection> ecList = weightListItr.next();

				if (ecList.contains(sel)) {
					ecList.remove(sel);
					if (ecList.isEmpty())
						m_weightRegistry.remove(((EventConsumerInfo) sel)
								.getAdvertisement().getWeight());
					LOGGER.info(
							"removing selection - " + sel.toString());
					ecList.remove(sel);
					return;
				}
			}

			for (int i = 0; i < m_weightedSelectionList.size(); i++) {
				if (m_weightedSelectionList.get(i).size() == 0)
					m_weightRegistry.remove(i);
			}
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
		 */
		public void writeExternal(ObjectOutput out) throws IOException {

			out.writeObject(m_weightRegistry);
			out.writeObject(m_weightedSelectionList);

		}

	}

	/**
     * 
     */
	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");

	private ConcurrentHashMap<JetstreamTopic, EventTopicRegistry.SelectionList> m_eventTopicRegistry = new ConcurrentHashMap<JetstreamTopic, EventTopicRegistry.SelectionList>(); // key
	// eventconsumerinfo.
	private int m_capacity = 101; // max entries in array list for each topic

	/**
	 * 
	 */
	public EventTopicRegistry() {
	}

	/**
	 * @param capacity
	 */
	public EventTopicRegistry(int capacity) {
		m_capacity = capacity;

	}

	/**
	 * @param topic
	 * @return
	 */
	public boolean containsKey(JetstreamTopic topic) {
		return m_eventTopicRegistry.containsKey(topic);
	}

	/**
	 * @param topic
	 * @return
	 */
	public ArrayList<LinkedList<Selection>> get(JetstreamTopic topic) {
		if (m_eventTopicRegistry.containsKey(topic)) {
			SelectionList sl = m_eventTopicRegistry.get(topic);
			if (sl != null) {
				return sl.getWeightedSelectionList();
			}
		}

		return null;
	}

	/**
	 * @param topic
	 * @return
	 */
	public WeightRegistry getWeightRegistry(JetstreamTopic topic) {
		if (m_eventTopicRegistry.containsKey(topic)) {
			SelectionList sl = m_eventTopicRegistry.get(topic);
			if (sl != null){
				return sl.getWeightRegistry();
			}
		}
		
		return null;
	}

	/**
	 * @param topic
	 * @param ecinfo
	 */
	public void put(JetstreamTopic topic, EventConsumerInfo ecinfo) {
		int weight = ecinfo.getAdvertisement().getWeight();

		if (weight > m_capacity) {
			LOGGER.error(
					"Weight registry capacity exceeded - max capacity = "
							+ m_capacity);
			return;
		}

		if (m_eventTopicRegistry.containsKey(topic)) {
			SelectionList selList = m_eventTopicRegistry.get(topic);

			if (selList == null) return;
			
			LinkedList<Selection> consumerList = selList
					.getWeightedSelectionList().get(weight);

			Iterator<Selection> itr = consumerList.iterator();

			EventConsumerInfo consumerToBeRemoved = null;

			while (itr.hasNext()) {
				EventConsumerInfo info = (EventConsumerInfo) itr.next();

				if (info.getAdvertisement().getHostAndPort()
						.equals(ecinfo.getAdvertisement().getHostAndPort())) {
					consumerToBeRemoved = info;
				}
			}

			if (consumerToBeRemoved != null)
				selList.remove(consumerToBeRemoved);

			selList.add(ecinfo, weight);
			
			WeightRegistry wr = selList.getWeightRegistry();
			
			if (wr != null)
				LOGGER.info( selList.getWeightRegistry().toString());
			
		} else {
			SelectionList selList = new SelectionList(m_capacity);

			selList.add(ecinfo, weight);

			m_eventTopicRegistry.put(topic, selList);

		}


	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
	 */
	@SuppressWarnings("unchecked")
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {

		m_eventTopicRegistry = (ConcurrentHashMap<JetstreamTopic, SelectionList>) in
				.readObject();

	}

	/**
	 * @param topic
	 * @param ecinfo
	 */
	public void remove(JetstreamTopic topic, EventConsumerInfo ecinfo) {
		if (m_eventTopicRegistry.containsKey(topic)) {

			SelectionList selList = m_eventTopicRegistry.get(topic);
			if (selList == null) return;
			
			LOGGER.info( selList.getWeightRegistry().toString());
			try {
				SelectionList sl = m_eventTopicRegistry.get(topic);
				if (sl != null)
					sl.remove(ecinfo);
			} catch (Throwable t) {}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
	 */
	public void writeExternal(ObjectOutput out) throws IOException {

		out.writeObject(m_eventTopicRegistry);

	}
}
