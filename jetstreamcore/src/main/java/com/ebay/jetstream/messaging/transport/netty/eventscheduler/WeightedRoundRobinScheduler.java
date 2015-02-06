/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.eventscheduler;

import java.util.ArrayList;
import java.util.LinkedList;

import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;
import com.ebay.jetstream.messaging.transport.netty.eventproducer.EventConsumerInfo;
import com.ebay.jetstream.messaging.transport.netty.registry.Registry;
import com.ebay.jetstream.messaging.transport.netty.registry.WeightRegistry;
import com.ebay.jetstream.messaging.transport.netty.schedulingalgorithm.lb.Selection;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 * 
 *          This is a type of scheduler that implements a weighted round robin scheduler. This class does not maintain a registry of its own.
 *          It references the registry that is passed to and references the topic registry to pull all consumers to get the weight assigned
 *          to each consumer.  
 * 
 */
public class WeightedRoundRobinScheduler implements Scheduler {

	static Object m_token = null;
	private long m_prevSelection = -1;
	private long m_currentWeight = 0;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.messaging.transport.netty.eventscheduler.Scheduler
	 * #scheduleNext(com.ebay.jetstream.messaging.messagetype.JetstreamMessage,
	 * com.ebay.jetstream.messaging.transport.netty.registry.Registry)
	 */
	@Override
	public EventConsumerInfo scheduleNext(JetstreamMessage msg,
			Registry registry) throws NoConsumerToScheduleException {

		EventConsumerInfo info = null;

		ArrayList<LinkedList<Selection>> weightedChoices = registry
				.getTopicRegistry().get(msg.getTopic());

		if (weightedChoices == null) {

			return null; // we might not have yet received any advertisement
		}

		Long indx = null;

		WeightRegistry wr = registry.getTopicRegistry().getWeightRegistry(
				msg.getTopic());

		if (wr.getWeights().size() <= 0)
			return null;

		while (true) {
			m_prevSelection = (m_prevSelection + 1) % wr.getWeights().size();
			if (m_prevSelection == 0) {
				m_currentWeight = m_currentWeight - wr.getWeightGcd();
				if (m_currentWeight <= 0) {
					m_currentWeight = wr.getMaxWeight();
					if (m_currentWeight == 0) // weight =0 means no traffic
						return null;
				}
			}
			if (wr.getWeights().get((int) m_prevSelection) >= m_currentWeight) {
				indx = wr.getWeights().get((int) m_prevSelection);
				break;
			}
		}

		LinkedList<Selection> choices; 		
		if(indx == null)
			choices = null;
		else
			choices = weightedChoices.get((int) indx
				.longValue());

		if (choices == null || choices.size() == 0)
			return (EventConsumerInfo) scheduleConsumerWithHighestWeight(weightedChoices);

		if (choices.size() == 1)
			return (EventConsumerInfo) choices.getFirst();

		Selection s = choices.removeFirst();

		s.setSelectionState(null);
		choices.getFirst().setSelectionState(m_token);
		choices.addLast(s);
		return (EventConsumerInfo) s;

	}

	/**
	 * @param weightedConsumerChoices
	 * @return
	 */
	private Selection scheduleConsumerWithHighestWeight(
			ArrayList<LinkedList<Selection>> weightedConsumerChoices) {

		LinkedList<Selection> choices = null;

		for (int i = weightedConsumerChoices.size() - 1; i > 0; i--) {
			choices = weightedConsumerChoices.get(i);

			if (choices == null || choices.size() == 0)
				continue;

			if (choices.size() == 1)
				return choices.getFirst();

			Selection s = choices.removeFirst();

			s.setSelectionState(null);

			choices.getFirst().setSelectionState(m_token);
			choices.addLast(s);
			return s;
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.messaging.transport.netty.eventscheduler.Scheduler
	 * #addEventCosumer
	 * (com.ebay.jetstream.messaging.transport.netty.eventproducer
	 * .EventConsumerInfo)
	 */
	@Override
	public void addEventConsumer(EventConsumerInfo info) {

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.messaging.transport.netty.eventscheduler.Scheduler
	 * #removeEventConsumer
	 * (com.ebay.jetstream.messaging.transport.netty.eventproducer
	 * .EventConsumerInfo)
	 */
	@Override
	public void removeEventConsumer(EventConsumerInfo info) {

	}

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.messaging.transport.netty.eventscheduler.Scheduler
	 * #supportsAffinity()
	 */
	@Override
	public boolean supportsAffinity() {
		return false;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ (int) (m_currentWeight ^ (m_currentWeight >>> 32));
		return result;
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

		if (!(obj instanceof WeightedRoundRobinScheduler))
			return false;

		WeightedRoundRobinScheduler cc = (WeightedRoundRobinScheduler) obj;

		
		if (m_currentWeight != cc.m_currentWeight)
			return false;
		
		return true;
	}
	
	
	/* (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	public  Scheduler clone() throws CloneNotSupportedException  {
		
		WeightedRoundRobinScheduler newScheduler = new WeightedRoundRobinScheduler();
		
		return newScheduler;
	
	}

}
