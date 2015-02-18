/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 *
 */
package com.ebay.jetstream.servlet;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ByteChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.mortbay.io.Buffer;
import org.mortbay.io.nio.ChannelEndPoint;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.HttpSchemes;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.nio.AbstractNIOConnector;
import org.mortbay.jetty.security.SslSocketConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.thread.BoundedThreadPool;
import org.mortbay.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextStartedEvent;
import org.springframework.context.event.ContextStoppedEvent;

import com.ebay.jetstream.common.ShutDownable;
import com.ebay.jetstream.config.ConfigUtils;
import com.ebay.jetstream.servlet.SSLServerHost;
import com.ebay.jetstream.servlet.ServletDefinition;

@SuppressWarnings("deprecation")
public class JettyServer implements ApplicationListener, InitializingBean, ShutDownable {

  static class SimpleNIOConnector extends AbstractNIOConnector {
    private transient ServerSocketChannel m_acceptChannel;
    private final InetAddress m_bindAddress;

    public SimpleNIOConnector(InetAddress address, int port) {
      m_bindAddress = address;
      setHost(m_bindAddress.getHostName());

      setPort(port);
      setUseDirectBuffers(false);
    }

    @Override
    protected void accept(int acceptorID) throws IOException, InterruptedException {
      SocketChannel channel = m_acceptChannel.accept();
      channel.configureBlocking(true);
      Socket socket = channel.socket();
      configure(socket);
      SimpleNIOHttpEndPoint endpoint = new SimpleNIOHttpEndPoint(this, channel);
      endpoint.dispatch();
    }

    public void close() throws IOException {
      if (m_acceptChannel != null) {
        m_acceptChannel.close();
      }
      m_acceptChannel = null;
    }

    @Override
    protected void connectionClosed(HttpConnection connection) {
      super.connectionClosed(connection);
    }

    @Override
    protected void connectionOpened(HttpConnection connection) {
      super.connectionOpened(connection);
    }

    public Object getConnection() {
      return m_acceptChannel;
    }

    public int getLocalPort() {
      return m_acceptChannel == null || !m_acceptChannel.isOpen() ? -1 : m_acceptChannel.socket().getLocalPort();
    }

    public void open() throws IOException {
      // Create a new server socket and set to blocking mode as for now
      m_acceptChannel = ServerSocketChannel.open();
      m_acceptChannel.configureBlocking(true);
      // Bind the server socket to the address and port
      InetSocketAddress addr = new InetSocketAddress(m_bindAddress, getPort());
      setAcceptQueueSize(100);
      setIntegralScheme(HttpSchemes.HTTP);
      setConfidentialScheme(HttpSchemes.HTTP);
      m_acceptChannel.socket().bind(addr, getAcceptQueueSize());
    }
  }

  static class SimpleNIOHttpEndPoint extends ChannelEndPoint implements Runnable {
    private int m_sotimeout;
    private final HttpConnection m_connection;
    private final SimpleNIOConnector m_connector;

    public SimpleNIOHttpEndPoint(SimpleNIOConnector connector, ByteChannel channel) {
      super(channel);
      m_connector = connector;
      m_connection = new HttpConnection(connector, this, connector.getServer());
    }

    protected void connectionClosed() {
      m_connector.connectionClosed(m_connection);
    }

    protected void connectionOpened() {
      m_connector.connectionOpened(m_connection);
    }

    public void dispatch() throws IOException {
      if (!m_connector.getThreadPool().dispatch(this)) {
        LOGGER.warn( "dispatch failed for " + m_connection.toString());
        close();
      }
    }

    @Override
    public int fill(Buffer buffer) throws IOException {
      int len = super.fill(buffer);
      if (len < 0)
        getChannel().close();
      return len;
    }

    public void run() {
      try {
        connectionOpened();
        while (isOpen()) {
          if (m_connection.isIdle()) {
            if (m_connector.getServer().getThreadPool().isLowOnThreads()) {
              if (m_sotimeout != m_connector.getLowResourceMaxIdleTime()) {
                m_sotimeout = m_connector.getLowResourceMaxIdleTime();
                ((SocketChannel) getChannel()).socket().setSoTimeout(m_sotimeout);
              }
            }
          }
          // Processing Http Request
          m_connection.handle();
        }
      }
      catch (Exception e) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug( "Failed", e);
        }
        try {
          close();
        }
        catch (IOException e1) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug( "closing", e1);
          }
        }
      }
      finally {
        connectionClosed();
      }
    }

  }

  private final String LOGGING_COMPONENT = this.getClass().getSimpleName();

  /**
   * Wanted to name it SSLHost but the spring init order was loading this after the servlet def (which infact required
   * this host to be loaded early on)
   */
  private SSLServerHost m_httpsHost;

  private static final Logger LOGGER = LoggerFactory.getLogger(JettyServer.class.getName());

  private boolean m_lazy = true;

  private boolean m_immediateStart = false;

  private InetAddress m_address;

  private int m_port;

  private int m_minThreadPoolSize = 3;

  private int m_maxThreadPoolSize = 9;

  private boolean m_boundedThreadPool = true;

  private String m_contextPath = "/";

  private String m_resourceBase;
  private final List<ServletDefinition> m_servletDefinitions = new ArrayList<ServletDefinition>();
  private Server m_server;
  private ContextHandlerCollection m_contexts;
  private Context m_context;
  private String m_webAppcontextPath;

  public String getWebAppcontextPath() {
	return m_webAppcontextPath;
}

public void setWebAppcontextPath(String m_webAppcontextPath) {
	this.m_webAppcontextPath = m_webAppcontextPath;
}

public void afterPropertiesSet() throws Exception {
    initServletDefinitions();
    if (isImmediateStart()){
      start();
      m_context.start();
    } 
  }

  public InetAddress getAddress() {
    return m_address;
  }
 
	protected Context getContext() {
		if (getWebAppcontextPath() != null) {
			m_context = new WebAppContext(
					null,ConfigUtils
					.getInitialPropertyExpanded(getContextPath()));
			m_context.setResourceBase(getResourceBase());
			getContexts().addHandler(m_context);
			getServer().setHandler(getContexts());
		} else if (m_context == null) {
			getServer().setHandler(getContexts());
			m_context = new Context(m_contexts, getContextPath(),
					Context.SESSIONS);
			if (getResourceBase() != null) {
				m_context.setResourceBase(getResourceBase());
			}
		}
		return m_context;
	}

  public String getContextPath() {
    return m_contextPath;
  }

  protected ContextHandlerCollection getContexts() {
    if (m_contexts == null)
      m_contexts = new ContextHandlerCollection();

    return m_contexts;
  }

  public int getPendingEvents() {
    // TODO Auto-generated method stub
    return 0;
  }

  public SSLServerHost getHttpsHost() {
    return m_httpsHost;
  }

  public int getMaxThreadPoolSize() {
    return m_maxThreadPoolSize;
  }

  public int getMinThreadPoolSize() {
    return m_minThreadPoolSize;
  }

  public int getPort() {
    return m_port;
  }

  public String getResourceBase() {
    return m_resourceBase;
  }

 
  public Server getServer() {
    if (m_server == null) {

      /*
       * try { m_address = InetAddress.getLocalHost(); // temporary to testSimpleNIOConnector } catch
       * (UnknownHostException e) {
       * 
       * e.printStackTrace(); }
       */

      InetAddress addr = getAddress();

      int port = getPort();

      if (addr == null)
        m_server = new Server(port);
      else
        m_server = new Server();

      if (getHttpsHost() != null)
        m_server.setConnectors(new Connector[] { getSSLConnector() });
      else if (addr != null) {
    	  LOGGER.warn( "SSL Host NOT Configured. Using Http");
        m_server.setConnectors(new Connector[] { new SimpleNIOConnector(addr, port) });
      }

      if (isBoundedThreadPool()) {
        BoundedThreadPool threadPool = new BoundedThreadPool();
        threadPool.setMinThreads(getMinThreadPoolSize());
        threadPool.setMaxThreads(getMaxThreadPoolSize());
        m_server.setThreadPool(threadPool);
      }
      else {
        QueuedThreadPool threadPool = new QueuedThreadPool();
        threadPool.setMinThreads(getMinThreadPoolSize());
        threadPool.setMaxThreads(getMaxThreadPoolSize());
        threadPool.setSpawnOrShrinkAt(getMinThreadPoolSize());
        m_server.setThreadPool(threadPool);
      }

    }
    return m_server;
  }

  /**
   * @return the servletDefinitions
   */
  public List<ServletDefinition> getServletDefinitions() {
    return m_servletDefinitions;
  }

  private Connector getSSLConnector() {
    SslSocketConnector sslConnector = new SslSocketConnector();
    sslConnector.setPort(getPort());
    sslConnector.setKeyPassword(getHttpsHost().getKeyStorePassword());
    sslConnector.setKeystore(getHttpsHost().getKeyStorePath());
    sslConnector.setTruststore(getHttpsHost().getTrustStorePath());
    sslConnector.setTrustPassword(getHttpsHost().getTrustStorePassword());
    return sslConnector;
  }

  /**
   * @param servletDefinitions
   *          the servletDefinitions to set
   * @throws ClassNotFoundException
   */
  private void initServletDefinitions() {
    try {
      Context context = getContext();
      int order = isLazy() ? -1 : 0;
      for (ServletDefinition sd : m_servletDefinitions) {
        ServletHolder holder = new ServletHolder(sd.getServletClass());
        if (order >= 0)
          order++;
        holder.setInitOrder(order);
        Map<String, String> initParams = sd.getInitParams();
        if (initParams != null)
          holder.setInitParameters(initParams);
        context.addServlet(holder, sd.getUrlPath());
        
      }
    }
    catch (Exception e) {
    	LOGGER.error( e.getMessage(), e);
    }

  }

  public boolean isBoundedThreadPool() {
    return m_boundedThreadPool;
  }

  public boolean isImmediateStart() {
    return m_immediateStart;
  }

  public boolean isLazy() {
    return m_lazy;
  }

  public void join() {
    try {
      getServer().join();
    }
    catch (InterruptedException e) {
      LOGGER.warn( e.getMessage());
    }
  }

  public void onApplicationEvent(ApplicationEvent event) {
    if (event instanceof ContextStartedEvent && !isImmediateStart()) {
      try {
        start();
      }
      catch (Exception e) {
        LOGGER.error( e.getLocalizedMessage(),  e);
      }
    }
    else if (event instanceof ContextClosedEvent || event instanceof ContextStoppedEvent) {
      stop();
    }
  }

  public void setAddress(InetAddress address) {
    m_address = address;
  }

  public void setBoundedThreadPool(boolean boundedThreadPool) {
    m_boundedThreadPool = boundedThreadPool;
  }

  public void setContextPath(String contextPath) {
    m_contextPath = contextPath;
  }

  public void setHttpsHost(SSLServerHost httpsHost) {
    m_httpsHost = httpsHost;
  }

  public void setImmediateStart(boolean earlyStart) {
    m_immediateStart = earlyStart;
  }

  public void setLazy(boolean lazy) {
    m_lazy = lazy;
  }

  public void setMaxThreadPoolSize(int maxThreadPoolSize) {
    m_maxThreadPoolSize = maxThreadPoolSize;
  }

  public void setMinThreadPoolSize(int minThreadPoolSize) {
    m_minThreadPoolSize = minThreadPoolSize;
  }

  public void setPort(int httpListenerPort) {
    m_port = httpListenerPort;
  }

  public void setResourceBase(String resourceBase) {
    m_resourceBase = ConfigUtils.getInitialPropertyExpanded(resourceBase);
  }

  public void setServletDefinitions(List<ServletDefinition> servletDefinitions) {
    m_servletDefinitions.clear();
    m_servletDefinitions.addAll(servletDefinitions);
  }

  public void shutDown() {
    stop();
  }

  public void start() throws Exception {
    try {
      LOGGER.warn( "Starting Jetty Server.");
      getServer().start();
    }
    catch (Exception e) {
      LOGGER.error( "Failed to start Jetty: " + e);
      throw e;
    }
  }

  public void stop() {
    try {
      getServer().stop();
    }
    catch (Exception e) {
      LOGGER.error( "Failed to stop Jetty: " + e);
    }
  }
}