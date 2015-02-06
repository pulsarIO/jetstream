/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.http;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.ebay.jetstream.http.netty.client.HttpClient;
import com.ebay.jetstream.http.netty.client.HttpClientConfig;
import com.ebay.jetstream.http.netty.server.HttpServer;
import com.ebay.jetstream.http.netty.server.HttpServerConfig;
import com.ebay.jetstream.servlet.ServletDefinition;

/**
 * Junit Test Case for Async HTTP Messaging
 * 
 * @author shmurthy
 * 
 */
public class HttpBasedMessagingTest implements HttpMsgTest{
	

	private static int 	SEND_COUNT = 1;
	public HttpBasedMessagingTest() {

	}

	AtomicBoolean m_testPassed = new AtomicBoolean(false);
	int port = new SecureRandom().nextInt(65539);
	
	@Override
	public AtomicBoolean getTestPassed() {
		return m_testPassed;
	}

	@Override
	public void setTestPassed(AtomicBoolean m_testPassed) {
		this.m_testPassed = m_testPassed;
	}

	public void main(String[] args) throws Exception {

	}
	
	protected HttpClient setupHttpClient(String encoder) {
		
		HttpClientConfig cc = new HttpClientConfig();
		
		cc.setBatchSz(1);
		cc.setNumConnections(1);
		cc.setNumWorkers(1);
		if(encoder != null)
			cc.setCompressEncoder(encoder);
		
		HttpClient client = new HttpClient();
		client.setConfig(cc);
		client.setKeepAlive(true);
		
		return client;
	}
	
	protected HttpServer setupHttpServer(HttpMsgListener listener) {
		
		HttpServerConfig sc = new HttpServerConfig();
		sc.setPort(port);
		sc.setServletExecutorThreads(1);
		sc.setNumIOWorkers(1);
		ArrayList<ServletDefinition> servletDefinitions = new ArrayList<ServletDefinition>();
		//servletDefinitions.add(new TestServlet())
		try {
			sc.setServletDefinitions(servletDefinitions);
		} catch (InstantiationException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IllegalAccessException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		HttpServer server = new HttpServer();
		server.setServerConfig(sc);
		
		
		try {

			server.afterPropertiesSet(); // this will start the server
			server.add("/test", new TestServlet(listener));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return server;
	}
	

	public void testHttpMessaging(String encoder) {

		System.out.println("Starting Http Messaging Test"); //KEEPME
		
		HashMap<String, String> map = new HashMap<String, String>();

		for(int i=0;i<100;i++)
			map.put("a"+i, "v"+i);
        
		HttpMsgListener listener = new HttpMsgListener(map, this);
		
		listener.setCount(0);

		HttpServer server = setupHttpServer(listener);
		listener.setEncoder(encoder);
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		HttpClient client = setupHttpClient(encoder);
		
		URI uri = null;
		try {
            
			uri = new URI("http://"+InetAddress.getLocalHost().getHostAddress()+":" + port + "/test");
			client.connect(uri, 1);
		} catch (URISyntaxException e) {
			assertTrue("Test Failed : " + e.getLocalizedMessage(), true);
		} catch (UnknownHostException e1) {
			assertTrue("Test Failed : " + e1.getLocalizedMessage(), true);
		}
		
		for (int i=0; i < SEND_COUNT; i++) {
			try {
				client.post(uri, map, null, null);
			} catch (Exception e) {
				System.out.println("didnot post");
				e.printStackTrace();
				assertTrue("Test Failed : " + e.getLocalizedMessage(), true);
			}
		}
		
		int count = SEND_COUNT;
		while (listener.getCount() <= 0) {
			
			try {
				if (count-- == 0) break;
				Thread.sleep(200);
				
			} catch (InterruptedException e) {
				assertTrue("Test Failed : " + e.getLocalizedMessage(), true);
			}
		}
		
		assertEquals(SEND_COUNT, listener.getCount());

		assertTrue("test failed!", getTestPassed().get());
		
		client.shutdown();
		server.shutDown();
	}
	
	@Test
	public void testHttpMessagingGzip(){
		testHttpMessaging("gzip");
	}
	
	@Test
	public void testHttpMessagingDeflate(){
		testHttpMessaging("deflate");
	}
	
	@Test
	public void testHttpMessaging(){
		testHttpMessaging(null);
	}


}
