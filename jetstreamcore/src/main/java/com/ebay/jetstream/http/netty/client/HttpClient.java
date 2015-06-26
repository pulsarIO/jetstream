/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.timeout.IdleStateHandler;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.common.NameableThreadFactory;
import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.messaging.MessageServiceTimer;
import com.ebay.jetstream.util.Request;
import com.ebay.jetstream.util.disruptor.SingleConsumerDisruptorQueue;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * 
 * @author shmurthy@ebay.com (shmurthy@ebay.com)
 * HttpClient works off of a queue in its own thread. This design is picked mainly to avoid any locking issues. There is a
 * data queue and a control queue. Control queue is processed at a higher priority.
 */
public class HttpClient extends Thread implements ChannelFutureListener,
		XSerializable {

	private static final class HttpResult {
	    private boolean isTimeOut;
	    private HttpResponse response;
	    
	}
	private static final class SyncResponseFuture implements ResponseFuture {
        private final CountDownLatch latch;
        private final HttpResult result;

        private SyncResponseFuture(CountDownLatch latch, HttpResult result) {
            this.latch = latch;
            this.result = result;
        }

        @Override
        public void operationComplete(HttpResponse response) {
            result.response = response;
            latch.countDown();
        }

        @Override
        public void setFailure() {
        }

        @Override
        public void setSuccess() {
        }

        @Override
        public void setTimedout() {
            result.isTimeOut = true;
        }
    }
	private static final Logger LOGGER = LoggerFactory
			.getLogger("com.ebay.jetstream.http.netty.client");
	private NioEventLoopGroup m_workerGroup;
	private Bootstrap m_bootstrap;
	private final HttpResponseHandler m_httpRequestHandler;
	private final ObjectMapper m_mapper = new ObjectMapper();
	private final ConcurrentHashMap<URI, HttpConnectionRegistry> m_urlConns = new ConcurrentHashMap<URI, HttpConnectionRegistry>();
	private final AtomicBoolean m_pipelineCreated = new AtomicBoolean(false);
	private final HttpResponseDispatcher m_responseDispatcher;
	private HttpClientConfig m_config = new HttpClientConfig();
	private AtomicBoolean m_shutDown = new AtomicBoolean(false);
	private BlockingQueue<Request> m_dataQueue = null;
	private final BlockingQueue<Request> m_controlQueue = new SingleConsumerDisruptorQueue<Request>(
			3000);
	private LongCounter m_totalEventsDropped = new LongCounter();
	private ConcurrentHashMap<String, LongCounter> m_urlDropCounters = new ConcurrentHashMap<String, LongCounter>();
	private LongCounter m_totalEventsSent = new LongCounter();
	private AtomicBoolean m_started = new AtomicBoolean(false);
	
	private boolean keepAlive = false;

	private int m_workQueueCapacity = 10000;
	
    private final CountDownLatch endLatch = new CountDownLatch(1);

    public Map<String, LongCounter> getUrlDropCounters() {
		return m_urlDropCounters;
	}
    
	public HttpClient() {
		super("Jetstream-HttpClient");
		m_httpRequestHandler = new HttpResponseHandler(this);
		m_responseDispatcher = new HttpResponseDispatcher(MessageServiceTimer
				.sInstance().getTimer(), getConfig().getDispatchWorkQueueSz(),
				getConfig().getDispatchThreadPoolSz()); // use Message
														// service timer for
														// now.
		m_dataQueue = new SingleConsumerDisruptorQueue<Request>(m_workQueueCapacity);

	}

	public HttpClient(int workQueueSz) {
		super("Jetstream-HttpClient");
		m_httpRequestHandler = new HttpResponseHandler(this);
		m_responseDispatcher = new HttpResponseDispatcher(MessageServiceTimer
				.sInstance().getTimer(), getConfig().getDispatchWorkQueueSz(),
				getConfig().getDispatchThreadPoolSz()); 
		m_workQueueCapacity = workQueueSz; 
		m_dataQueue = new LinkedBlockingQueue<Request>(m_workQueueCapacity);

	}

	private HttpSessionChannelContext activateHttpSession(URI uri)
			throws UnknownHostException {

		ChannelFuture cf = null;
		try {
			cf = m_bootstrap.connect(new InetSocketAddress(InetAddress
					.getByName(uri.getHost()), uri.getPort()));
			
			if (!m_urlDropCounters.containsKey(uri.getHost()))
				m_urlDropCounters.put(uri.getHost(), new LongCounter());
			
		} catch (UnknownHostException e) {
			LOGGER.error(
					"failed to connect to host" + e.getLocalizedMessage());
			throw e;
		}

		cf.awaitUninterruptibly((getConfig().getConnectionTimeoutInSecs() + 1) * 1000);

		if (!cf.channel().isActive()) {
			return null;
		}
		
        HttpSessionChannelContext sessionContext = new HttpSessionChannelContext();
        sessionContext.setChannel(cf.channel());
        sessionContext.channelConnected();
        sessionContext.setUri(uri.toString());
        sessionContext.getVirtualQueueMonitor().setMaxQueueBackLog(getConfig().getMaxNettyBacklog());
        sessionContext.setSessionDurationInSecs(getConfig().getMaxSessionDurationInSecs());
        return sessionContext;

	}

	public void channelDisconnected(Channel channelid) {

		ProcessChannelDisconnectRequest pcdr = new ProcessChannelDisconnectRequest(this, channelid);
		
		try {
			m_controlQueue.offer(pcdr);
		} catch (Throwable t) {
			LOGGER.error( "Failed to insert ProcessChannelDisconnectRequest to control queue" + t.getLocalizedMessage());
		}
		
		ControlMsgReadRequest cmr = new ControlMsgReadRequest();
		
		m_dataQueue.offer(cmr);

		
	}

	public void close() {

		m_urlConns.clear(); // hopefully this will close all connections

	}

	/**
 * 
 */
	private synchronized void closeAllConnections() {

		Enumeration<HttpConnectionRegistry> connRegistries = m_urlConns
				.elements();

		while (connRegistries.hasMoreElements()) {

			HttpConnectionRegistry connRegistry = connRegistries.nextElement();

			Collection<HttpSessionChannelContext> sessions = connRegistry
					.getAllSessions();

			Iterator<HttpSessionChannelContext> itr = sessions.iterator();

			while (itr.hasNext()) {
				HttpSessionChannelContext context = itr.next();

				context.getChannel().close();
			}
		}

	}

	public void connect(URI uri, int numConnections)
			throws UnknownHostException {

		if (!isPipelineCreated()) {
			
			createChannelPipeline();
		
		}

		HttpConnectionRegistry conRegistry = getConnectionRegistry(uri);

		if (conRegistry.isEmpty()) {
			conRegistry.setMaxConnections(numConnections);
			conRegistry.setUri(uri);
			for (int i = 0; i < numConnections; i++) {
				HttpSessionChannelContext channelcontext = activateHttpSession(uri);

				if (channelcontext != null)
					conRegistry.add(channelcontext);
			}
		}
	}

	private void createChannelPipeline() {

		if (isPipelineCreated())
			return;
        
        m_workerGroup = new NioEventLoopGroup(getConfig().getNumWorkers(), new NameableThreadFactory("Jetstream-HttpClientWorker"));
        m_bootstrap = new Bootstrap();
        m_bootstrap.group(m_workerGroup).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, false)
            .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS,  getConfig()
                .getConnectionTimeoutInSecs())
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast("timeout", new IdleStateHandler(0, getConfig().getIdleTimeoutInSecs(), 0));
                        ch.pipeline().addLast("decoder", new HttpResponseDecoder());
                        ch.pipeline().addLast("decompressor", new HttpContentDecompressor());
                        ch.pipeline().addLast("encoder", new HttpRequestEncoder());
                        ch.pipeline().addLast("aggregator", new HttpObjectAggregator(m_config.getMaxContentLength()));
                        ch.pipeline().addLast(m_httpRequestHandler);
                    }
                });
		
        if (getConfig().getRvcBufSz() > 0) {
            m_bootstrap.option(ChannelOption.SO_RCVBUF, (int) getConfig().getRvcBufSz());
        }
        
        if ( getConfig().getSendBufSz() > 0) {
            m_bootstrap.option(ChannelOption.SO_SNDBUF, (int) getConfig().getSendBufSz());
        }
		createdPipeline();

	}
	
	private void createdPipeline() {
		m_pipelineCreated.set(true);
	}

	public void dispatchResponse(String requestid, HttpResponse response) {
		if (m_responseDispatcher != null)
			m_responseDispatcher.dispatch(requestid, response);
	}

	/**
	 * @return the config
	 */
	public HttpClientConfig getConfig() {
		return m_config;
	}

	private HttpConnectionRegistry getConnectionRegistry(URI uri) {

		if (!m_urlConns.containsKey(uri)) {
			HttpConnectionRegistry registry = new HttpConnectionRegistry();
			registry.setMaxConnections(m_config.getNumConnections());
            m_urlConns.put(uri, registry);
		}

		return m_urlConns.get(uri);

	}
	
	
	private boolean manageConnections(HttpConnectionRegistry conRegistry, URI uri) {
		if (conRegistry.isEmpty()) {
			// lets call connect here
			try {
				connect(uri, conRegistry.getMaxConnections());
			} catch (UnknownHostException e) {
				LOGGER.error( e.getMessage(), e);
				return false;
			}
		} else if (conRegistry.getSessionCount() < conRegistry
				.getMaxConnections()) {

			int newConnections = getConfig().getNumConnections()
					- conRegistry.getSessionCount();
			
			// now we will increase number of connections to bump up to maxConnections
			
			for (int i = 0; i < newConnections; i++) {
				HttpSessionChannelContext channelcontext;
				try {
					channelcontext = activateHttpSession(conRegistry.getUri());
					if (channelcontext != null)
						conRegistry.add(channelcontext);
				} catch (UnknownHostException e) {
					LOGGER.error( e.getMessage(), e);
				}
			}
			
		}
		
		return true;
	}

	private HttpSessionChannelContext getNextSession(URI uri) {

		HttpConnectionRegistry conRegistry = getConnectionRegistry(uri);

		manageConnections(conRegistry, uri);

		HttpSessionChannelContext ctx = conRegistry.getNextSession();
		
		if (ctx == null) {
		
			manageConnections(conRegistry, uri);
		
			ctx = conRegistry.getNextSession(); // could be session closed by the time
			// we fetch session. So we will try to make connection one more time
		}
		
		return conRegistry.getNextSession();
		
	}
	
    public long getTotalResponseRejected() {
        return m_responseDispatcher.getQueueFullCounter();
    }
    
	public long getTotalResponseReceived() {
	    return m_responseDispatcher.getResponseCounter();
	}
	
    public long getTotalTimeoutRequests() {
        return m_responseDispatcher.getTimeoutCounter();
    }
	   
    public long getTotalResponseDropped() {
        //drop when response timedout
        return m_responseDispatcher.getDropCounter();
    }
    
    public long getTotalEventsDropped(boolean reset) {
    	
    	if (reset) {
    		return m_totalEventsDropped.getAndReset();
    	}
    	else
    		return m_totalEventsDropped.get();
    	
    }

    public long getTotalEventsSent() {
        return m_totalEventsSent.get();
    }
      
	public int getWorkQueueCapacity() {
		return m_workQueueCapacity;
	}

    public int getQueueBacklog() {
        return m_dataQueue.size();
    }
    
	/**
	 * @return the keepAlive
	 */
	public boolean isKeepAlive() {
		return keepAlive;
	}
	
	private boolean isPipelineCreated() {
		return m_pipelineCreated.get();
	}

    @Override
	public void operationComplete(ChannelFuture future) throws Exception {
		if (future instanceof HttpSessionChannelFuture) {

			HttpSessionChannelFuture sf = (HttpSessionChannelFuture) future;

			sf.getVirtualQueueMonitor().decrement();

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(
						"Operation Complete - success = " + sf.isSuccess());
			}

			if (!sf.isSuccess()) {
				// we submit the message back if TTL has not expired.

				int ttl = sf.decrementAndGetTTL();

				if (ttl > 0) {
					if (!sf.getVirtualQueueMonitor().isQueueFull()) {

						if (m_dataQueue.size() < m_workQueueCapacity) {
							ProcessHttpWorkRequest workRequest = new ProcessHttpWorkRequest(
									this, sf.getUri(), sf.getMessage());

							m_dataQueue.offer(workRequest);
						} else {
							m_totalEventsDropped.increment();

						}

					}
				}

			}

		}

	}

	/*
	 * post - non-blocking call which transmits the request Asynchronously also
	 * returns the response to he caller asynchrounously through a registered
	 * future. This method executes a HTTP POST to server with content in JSON
	 * form.
	 * 
	 * @param content - java object to be serialized as JSON
	 * 
	 * @param headers - optional custom http headers
	 * 
	 * @param responsefuture - optional future if response is to be delivered
	 * asynchronously
	 * 
	 * @throws IOException
	 */
	public void post(URI uri, Object content, Map<String, String> headers,
			ResponseFuture responsefuture) throws Exception {

		if (m_shutDown.get()) {
			throw new IOException("IO has been Shutdown = ");

		}

		if (uri == null)
			throw new NullPointerException("uri is null or incorrect");

		if (!m_started.get()) {
			synchronized (this) {
				if (!m_started.get()) {
					start();
					m_started.set(true);
				}
			}
		}
		
	    ByteBuf byteBuf = Unpooled.buffer(m_config.getInitialRequestBufferSize());
	    ByteBufOutputStream outputStream = new ByteBufOutputStream(
                byteBuf);

        // transform to json
        try {
            m_mapper.writeValue(outputStream, content);
        } catch (JsonGenerationException e) {
            throw e;
        } catch (JsonMappingException e) {
            throw e;
        } catch (IOException e) {
            throw e;
        }
        finally {
        	outputStream.close();
        }

        EmbeddedChannel encoder;
        String contenteEncoding;
        if ("gzip".equals(m_config.getCompressEncoder())) {
            encoder = new EmbeddedChannel(ZlibCodecFactory.newZlibEncoder(
                ZlibWrapper.GZIP, 1));
            contenteEncoding = "gzip";
            
        } else if ("deflate".equals(m_config.getCompressEncoder())) {
            encoder = new EmbeddedChannel(ZlibCodecFactory.newZlibEncoder(
                    ZlibWrapper.ZLIB, 1));
            contenteEncoding = "deflate";
        } else {
            encoder = null;
            contenteEncoding = null;
        }
        
        if (encoder != null) {
            encoder.config().setAllocator(UnpooledByteBufAllocator.DEFAULT);
            encoder.writeOutbound(byteBuf);
            encoder.finish();
            byteBuf = (ByteBuf) encoder.readOutbound();
            encoder.close();
        }
        
        
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, HttpMethod.POST, uri.toString(), byteBuf);
        if (contenteEncoding != null) {
            HttpHeaders.setHeader(request, HttpHeaders.Names.CONTENT_ENCODING, contenteEncoding);
        }
        HttpHeaders.setHeader(request, HttpHeaders.Names.ACCEPT_ENCODING, "gzip, deflate");
        HttpHeaders.setHeader(request, HttpHeaders.Names.CONTENT_TYPE, "application/json");
        HttpHeaders.setContentLength(request, 
                byteBuf.readableBytes());
	    

		if (isKeepAlive())
		    HttpHeaders.setHeader(request, HttpHeaders.Names.CONNECTION, "keep-alive");
	
		
		if (headers != null) {
			@SuppressWarnings("rawtypes")
			Iterator it = headers.entrySet().iterator();
			while (it.hasNext()) {
				@SuppressWarnings("rawtypes")
				Map.Entry pairs = (Map.Entry) it.next();
				HttpHeaders.setHeader(request, (String) pairs.getKey(), pairs.getValue());
			}
		}
		
		

		if (responsefuture != null) {
			RequestId reqid = RequestId.newRequestId();
			
			m_responseDispatcher.add(reqid, responsefuture);
			
			HttpHeaders.setHeader(request, "X_EBAY_REQ_ID", reqid.toString()); 
			// we expect this to be echoed in the response used for correlation.
		}
		

		if (m_dataQueue.size() < m_workQueueCapacity) {
			ProcessHttpWorkRequest workRequest = new ProcessHttpWorkRequest(
					this, uri, request);

			if (!m_dataQueue.offer(workRequest)) {
			    if (responsefuture != null) {
			        responsefuture.setFailure();
			        m_responseDispatcher.remove(request.headers().get("X_EBAY_REQ_ID"));
			    }
			}
		} else {
			throw new IOException("downstream Queue is full ");
		}
		
	}

	private boolean processControlMessages() {
		while (true) {

			try {
				if (m_controlQueue.peek() == null)
					return true;

				Request req = m_controlQueue.take();
				if (req != null) {
					return req.execute();
				}

			} catch (Throwable t) {
				if (LOGGER.isDebugEnabled())
					LOGGER.debug(
							"failed to process control message"
									+ t.getMessage());

			}

		}
	}

	void processDisconnectedChannel(Channel channelId) {

		Set<Entry<URI, HttpConnectionRegistry>> registries = m_urlConns
				.entrySet();

		Iterator<Entry<URI, HttpConnectionRegistry>> itr = registries
				.iterator();

		// we will iterate through all key entries and remove - no harm in doing
		// that
		while (itr.hasNext()) {
			Entry<URI, HttpConnectionRegistry> entry = itr.next();

			entry.getValue().remove(channelId);

		}

	}

	void releaseAllResources() {
		closeAllConnections();

		m_workerGroup.shutdownGracefully();

		if (m_responseDispatcher != null)
			m_responseDispatcher.shutDown();
	
	}

	@Override
	public void run() {
		// We will run this HttpClient in a seperate thread. All requests are
		// made through a single work
		// queue. We will process the queue and execute the job in the context
		// of
		// this queue. This design is chosen to eliminate locking
		try {
			while (true) {

				// first reap dead consumers and then process work queue

				if (!processControlMessages())
					return;

				Request req = null;
				try {
					req = m_dataQueue.take();

					if (req != null && !req.execute()) {
						return;
					}
					req = null;
				} catch (InterruptedException e) {
					// Ignore
				} catch (Throwable e) {

					// we better not reach here. If we do we have a serious problem
					// requiring restart of the server

					String message = "Event Producer failed to execute task for context - ";
					message += " Caught Exception ";
					message += e.getMessage();

					LOGGER.error( message);
					req = null;
					continue;
				}
			}
		} finally {
			endLatch.countDown();
		}
	}

	/**
	 * @param config
	 *            the config to set
	 */
	public void setConfig(HttpClientConfig config) {
		this.m_config = config;
	}

	/**
	 * @param keepAlive
	 *            the keepAlive to set
	 */
	public void setKeepAlive(boolean keepAlive) {
		this.keepAlive = keepAlive;
	}

	public void setWorkQueueCapacity(int workQueueCapacity) {
		this.m_workQueueCapacity = workQueueCapacity;
		m_dataQueue = new SingleConsumerDisruptorQueue<Request>(m_workQueueCapacity);
	}

	public void shutdown() {

		m_shutDown.set(true);
		
		ShutdownRequest sr = new ShutdownRequest(this);

		boolean status = m_controlQueue.offer(sr);
		
		ControlMsgReadRequest cmr = new ControlMsgReadRequest();
		
		m_dataQueue.offer(cmr);

		try {
			endLatch.await(10, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	public HttpResponse syncPost(URI uri, Object content, Map<String, String> headers, int timeoutInMs)
            throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final HttpResult result = new HttpResult();
        ResponseFuture responseFuture = new SyncResponseFuture(latch, result);
        
        this.post(uri, content, headers, responseFuture);
        if (latch.await(timeoutInMs, TimeUnit.MILLISECONDS)) {
            if (result.response != null) {
                return result.response;
            } else if (result.isTimeOut) {
                throw new IOException("Http time out " + uri);    
            } else {
                throw new IOException("Http failure " + uri);
            }
        } else {
            throw new IOException("Http time out " + uri);
        }
    }

	void writeRequest(URI uri, HttpRequest request) {
		HttpSessionChannelContext channelcontext = getNextSession(uri);
		Channel channel;
		if (channelcontext != null) {
		    channel = channelcontext.getChannel();
		} else {
			m_totalEventsDropped.increment();
			m_urlDropCounters.get(uri.getHost()).increment();
			if (LOGGER.isDebugEnabled())
				LOGGER.debug(
					"event dropped! No connections available to"
							+ uri.toString());
			return;
		}

		if (channel != null && channel.isActive()) {

			HttpSessionChannelFuture future = new HttpSessionChannelFuture(
			        channel, channelcontext.getVirtualQueueMonitor());

			future.setMessage(request); // we need to add the message to the
										// future so we can retry the message in
										// case of
			// failure. Look at operationComplete()
			// to see how we handle this.

			future.setUri(uri);

			future.addListener(this);


			if (!channelcontext.getVirtualQueueMonitor().isQueueFull()) {

				channel.writeAndFlush(request, future);

				channelcontext.getVirtualQueueMonitor().increment();

				m_totalEventsSent.increment();

			} else {
				if (LOGGER.isDebugEnabled())
					LOGGER.debug(
							"Dropping message - virtual queue is full");
				m_totalEventsDropped.increment();
			}
		}
		
	}

}
