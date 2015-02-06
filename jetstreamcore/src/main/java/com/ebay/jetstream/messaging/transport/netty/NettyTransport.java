/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.config.ConfigException;
import com.ebay.jetstream.config.NICUsage;
import com.ebay.jetstream.config.dns.DNSMap;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.messaging.DispatchQueueStats;
import com.ebay.jetstream.messaging.MessageServiceProxy;
import com.ebay.jetstream.messaging.config.ContextConfig;
import com.ebay.jetstream.messaging.config.TransportConfig;
import com.ebay.jetstream.messaging.exception.MessageServiceException;
import com.ebay.jetstream.messaging.interfaces.ITransportListener;
import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;
import com.ebay.jetstream.messaging.stats.TransportStats;
import com.ebay.jetstream.messaging.stats.TransportStatsController;
import com.ebay.jetstream.messaging.topic.JetstreamTopic;
import com.ebay.jetstream.messaging.transport.AbstractTransport;
import com.ebay.jetstream.messaging.transport.netty.config.NettyContextConfig;
import com.ebay.jetstream.messaging.transport.netty.config.NettyTransportConfig;
import com.ebay.jetstream.messaging.transport.netty.eventconsumer.Acceptor;
import com.ebay.jetstream.messaging.transport.netty.eventconsumer.EventConsumer;
import com.ebay.jetstream.messaging.transport.netty.eventproducer.EventProducer;
import com.ebay.jetstream.messaging.transport.netty.eventproducer.EventProducerStats;
import com.ebay.jetstream.messaging.transport.netty.stats.RegistryStatsProvider;
import com.ebay.jetstream.util.NetMask;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 * 
 *          This is Netty adaptor for Jetstream Message Service.
 * 
 */

@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "BC_UNCONFIRMED_CAST")
public class NettyTransport extends AbstractTransport {

	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");
	private String m_ipaddr = "10.244.88.124";
	private int m_port = 14953;
	private String m_context = "";
	private final AtomicBoolean m_initialized = new AtomicBoolean(false);
	private com.ebay.jetstream.messaging.interfaces.ITransportListener m_tl = null;
	private final AtomicBoolean m_nettylistenerCreated = new AtomicBoolean(
			false);
	private EventProducer m_evproducer = null;
	private EventConsumer m_evconsumer = null;
	private List<InetAddress> m_ifcAddrList = new LinkedList<InetAddress>();;
	private NettyTransportConfig m_transportConfig;
	private MessageServiceProxy m_serviceProxy;
	private NettyContextConfig m_cc;
	private String m_advisoryContext;
	private ConcurrentHashMap<Integer, Integer> m_listeningPorts = new ConcurrentHashMap<Integer, Integer>();

	/**
	 * @return
	 * @throws Exception
	 * @throws ConfigException
	 */
	private boolean locateInterfaces() throws ConfigException, Exception {
	
		m_ifcAddrList = findIfcOnHost(m_transportConfig.getTransportName()
				+ "-servicemcast");

		if (m_ifcAddrList.size() > 0) {
			for (int i = 0; i < m_ifcAddrList.size(); i++) {

				if (LOGGER.isInfoEnabled()) {
					String message = "Jetstream Message Context - " + m_context
							+ " - Binding to Interface - "
							+ m_ifcAddrList.get(i).getHostAddress();

					LOGGER.info( message);
				}
			}
		} else {
			String message = "No Interfaces found for creating multicast transport for Jetstream Message Context";
			message += " " + m_context;

			LOGGER.error( message);

			return false;
		}

		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof NettyTransport)) {
			return false;
		}
		NettyTransport other = (NettyTransport) obj;
		if (m_ipaddr == null) {
			if (other.m_ipaddr != null) {
				return false;
			}
		} else if (!m_ipaddr.equals(other.m_ipaddr)) {
			return false;
		}
		if (m_port != other.m_port) {
			return false;
		}
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((m_ipaddr == null) ? 0 : m_ipaddr.hashCode());
		result = prime * result + m_port;
		return result;
	}

	/**
	 * @return
	 */

	private void registerDnsType(String serviceType) {

		NICUsage nic = getMessageServiceProperties().getNicUsage();

		try {

			nic.registerDnsAssignedType(serviceType);

		} catch (ConfigException e1) {

			String message = " received exception while getting multicast enabled interface from DNS - ";
			message += e1.getMessage();

			LOGGER.error( message);

		}
	}

	private void addNetMask(String serviceType, NetMask mask) throws Exception {

		NICUsage nic = getMessageServiceProperties().getNicUsage();

		nic.addNetMask(serviceType, mask);
		nic.addNICInfo(serviceType);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.Jetstream.messaging.ITransportProvider#getStats()
	 */
	public TransportStats getStats() {

		EventProducerStats stats = new EventProducerStats();

		stats.setContext(m_context);
		stats.setProtocol("TCP");

		if (m_evproducer != null) {
			EventProducerStats producerStats = (EventProducerStats) m_evproducer
					.getStats();

			stats.setMsgsSentPerSec(producerStats.getMsgsSentPerSec());
			stats.setTotalMsgsSent(producerStats.getTotalMsgsSent());
			stats.setTotalRawBytes(producerStats.getTotalRawBytes());
			stats.setTotalCompressedBytes(producerStats
					.getTotalCompressedBytes());
			stats.setTotalRequestsSent(producerStats.getTotalRequestsSent());
			stats.setEventConsumerRegistry(producerStats
					.getEventConsumerRegistry());
			stats.setAffinityRegistry(producerStats.getAffinityRegistry());
			stats.setTotalBytesSent(producerStats.getTotalBytesSent());
			stats.setTotalMsgsDropped(producerStats.getTotalMsgsDropped());
			stats.setDownStreamQueueBacklog(producerStats
					.getDownStreamQueueBacklog());
			stats.setContextConfig(producerStats.getContextConfig());
			stats.setTransportConfig(m_transportConfig);
			stats.setDropsForMissingAK(producerStats.getDropsForMissingAK());
			stats.setDropsForNoConsumer(producerStats.getDropsForNoConsumer());
			stats.setDropsForVQOverflow(producerStats.getDropsForVQOverflow());
			stats.setMissingAKAdvisories(producerStats.getMissingAKAdvisories());
			stats.setNoConsumerAdvisories(producerStats
					.getNoConsumerAdvisories());
			stats.setOtherMsgDropAdvisories(producerStats
					.getOtherMsgDropAdvisories());
			stats.setVqOverflowAdvisories(producerStats
					.getVqOverflowAdvisories());

		}

		if (m_evconsumer != null) {
			TransportStats consumerStats = m_evconsumer.getStats();

			stats.setMsgsRcvdPerSec(consumerStats.getMsgsRcvdPerSec());
			stats.setTotalMsgsRcvd(consumerStats.getTotalMsgsRcvd());
		}

		return stats;
	}

	/**
	 * @return
	 */
	public ITransportListener getTransportListener() {
		return m_tl;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.Jetstream.messaging.ITransportProvider#harvestStats()
	 */
	public void harvestStats() {

		// we will ignore this signal
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.Jetstream.messaging.ITransportProvider#init(com.ebay.Jetstream
	 * .messaging.TransportKeyEntry, com.ebay.Jetstream.config.NICUsage,
	 * com.ebay.Jetstream.config.dns.DNSMap,
	 * com.ebay.Jetstream.messaging.MessageServiceProxy)
	 */
	public void init(TransportConfig transportConfig, NICUsage nicUsage,
			DNSMap dnsMap, MessageServiceProxy proxy) throws Exception {
		if (!m_initialized.get()) {
			m_transportConfig = (NettyTransportConfig) transportConfig;
			m_serviceProxy = proxy;
			m_evproducer = new EventProducer();
			m_evproducer.setContextConfig(m_cc);
			m_evproducer.init(m_cc, m_port, transportConfig, proxy, this);
			m_evproducer.start();

			if ((!m_transportConfig.requireDNS()) && (m_cc.getPort() < 0))
				throw new Exception("port not set for context"
						+ m_cc.getContextname());

			if (!m_transportConfig.requireDNS())
				setPort(m_cc.getPort());

		}

		if (m_transportConfig.requireDNS()) {
			registerDnsType(m_transportConfig.getTransportName()
					+ "-servicemcast");
		} else {
			addNetMask(m_transportConfig.getTransportName() + "-servicemcast",
					new NetMask(m_transportConfig.getNetmask()));

		}

		locateInterfaces();

		// register with monitoring and control service
		Management.addBean("MessageService/Transport/Netty/context/"
				+ m_context + "/stats", new TransportStatsController(this));
		Management.addBean("MessageService/Transport/Netty/context/"
				+ m_context + "/registry", new RegistryStatsProvider(this));
		

		m_initialized.set(true);

	}

	public void pause(JetstreamTopic topic) {
		m_evconsumer.pause(topic);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.Jetstream.messaging.ITransportProvider#registerListener(com.
	 * ebay.Jetstream.messaging.TransportListener)
	 */
	public void registerListener(ITransportListener tl) throws Exception {

		m_tl = tl;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.Jetstream.messaging.ITransportProvider#registerTopic(com.ebay
	 * .Jetstream.messaging.JetstreamTopic)
	 */
	public void registerTopic(JetstreamTopic topic) {

		// The following code was moved here from registerListener. This will
		// enable us to
		// create executor filter threads in the consumer space only when we
		// have somebody
		// upstream registering interest in a topic

		if (topic.getTopicName().equals(m_advisoryContext)) {
			if (m_evproducer != null)
				m_evproducer.setAdvisoryListener(m_tl);
		} else {
			if (!m_nettylistenerCreated.get()) {

				m_evconsumer = new EventConsumer();
				try {
					m_evconsumer.init(m_cc, m_tl, m_ifcAddrList, m_port,
							m_transportConfig, m_serviceProxy);

					m_evproducer.SetEventConsumer(m_evconsumer);
					List<Acceptor> acceptors = m_evconsumer.getAcceptorList();

					for (Acceptor acceptor : acceptors) {
						m_listeningPorts.put(acceptor.getTcpPort(), (int) -1);
					}
				} catch (Exception e) {
					LOGGER.error(
							"Failed to create Event Consumer for context - "
									+ m_context + " Exception - "
									+ e.getMessage() + " - "
									+ Arrays.toString(e.getStackTrace()));
				}

				m_nettylistenerCreated.set(true);
			}

			m_evconsumer.registerTopic(topic);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.Jetstream.messaging.ITransportProvider#resetStats()
	 */
	public void resetStats() {

		if (m_evconsumer != null)
			m_evconsumer.resetStats();

		if (m_evproducer != null)
			m_evproducer.resetStats();

	}

	public void resume(JetstreamTopic topic) {
		m_evconsumer.resume(topic);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.Jetstream.messaging.ITransportProvider#send(com.ebay.Jetstream
	 * .messaging.JetstreamMessage)
	 */
	public void send(JetstreamMessage msg) throws Exception {
		m_evproducer.send(msg);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.Jetstream.messaging.ITransportProvider#setAddr(java.lang.String)
	 */
	public void setAddr(String addr) {
		m_ipaddr = addr;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.Jetstream.messaging.ITransportProvider#setContext(java.lang.
	 * String)
	 */
	public void setContextConfig(ContextConfig cc) {
		m_context = cc.getContextname();
		m_advisoryContext = m_context + "/InternalStateAdvisory";
		m_cc = (NettyContextConfig) cc;
		if (m_evproducer != null)
			m_evproducer.setContextConfig(m_cc);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.Jetstream.messaging.ITransportProvider#setPort(int)
	 */
	public void setPort(int port) {
		m_port = port;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.Jetstream.messaging.ITransportProvider#setUpstreamDispatchQueueStats
	 * (com.ebay.Jetstream.messaging. DispatchQueueStats)
	 */
	public void setUpstreamDispatchQueueStats(DispatchQueueStats stats) {

		if (m_evconsumer != null)
			m_evconsumer.setUpstreamDispatchQueueStats(stats);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.Jetstream.messaging.ITransportProvider#shutdown()
	 */
	public void shutdown() throws MessageServiceException {
		if (LOGGER.isInfoEnabled()) {
			String message = "shutting  down listener and transport for Jetstream Message Context - "
					+ m_context;

			LOGGER.info( message);
		}

		if (m_evproducer != null)
			m_evproducer.shutdown();

		if (m_evconsumer != null)
			m_evconsumer.shutdown();

		boolean status = Management
				.removeBeanOrFolder("MessageService/Transport/Netty/context/"
						+ m_context + "/stats");

		if (status)
			LOGGER.info(
					"stop monitoring MessageService/Transport/Netty/context/"
							+ m_context + "/stats");

		status = Management
				.removeBeanOrFolder("MessageService/Transport/Netty/context/"
						+ m_context + "/registry");

		if (status)
			LOGGER.info(
					"stop monitoring MessageService/Transport/Netty/context/"
							+ m_context + "/registry");

		status = Management
				.removeBeanOrFolder("MessageService/Transport/Netty/context/"
						+ m_context);

		if (status)
			LOGGER.info(
					"stop monitoring MessageService/Transport/Netty/context/"
							+ m_context);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.Jetstream.messaging.ITransportProvider#unregisterTopic(com.ebay
	 * .Jetstream.messaging.JetstreamTopic)
	 */
	public void unregisterTopic(JetstreamTopic topic) {

		m_evconsumer.unregisterTopic(topic);
	}

	@Override
	public ContextConfig getContextConfig() {

		return m_cc;
	}

	@Override
	public void prepareToPublish(String context) {

		m_evproducer.prepareToPublish(context);
	}

	@Override
	public TransportConfig getTransportConfig() {
		return m_transportConfig;
	}

	public boolean isListeningToPort(int port) {
		if (m_evconsumer == null)
			return false;
		else
			return m_listeningPorts.containsKey(port);
	}

}
