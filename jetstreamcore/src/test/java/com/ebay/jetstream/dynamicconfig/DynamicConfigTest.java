/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.dynamicconfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

import com.ebay.jetstream.config.ApplicationInformation;
import com.ebay.jetstream.config.RootConfiguration;
import com.ebay.jetstream.messaging.MessageService;
import com.ebay.jetstream.messaging.transport.netty.config.NettyTransportConfig;
import com.ebay.jetstream.messaging.transport.zookeeper.ZooKeeperTransportConfig;
import com.ebay.jetstream.testutil.TestZKServer;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.ArtifactStoreBuilder;
import de.flapdoodle.embed.mongo.config.DownloadConfigBuilder;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.RuntimeConfigBuilder;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.config.store.HttpProxyFactory;
import de.flapdoodle.embed.process.runtime.Network;

public class DynamicConfigTest {

	private DB db;
	private static MongodProcess mongod;
	private static  int mongoport = new SecureRandom().nextInt(65539);
	private String zkport = TestZKServer.getPort();

	private DBCollection collection;
	private String appname = "ConfigChange";
	private String version = "0.1";
	private String TEMPLATE_FILE = "src/test/java/com/ebay/jetstream/dynamicconfig/msp_template.xml";
	private String FILE_PATH = "src/test/java/com/ebay/jetstream/dynamicconfig/";
	private String CONFIG_ROOT = "src/test/java/com/ebay/jetstream/dynamicconfig/file";
	private Map<String, String> template_replace_map = new HashMap<String, String>();
	
	
	@BeforeClass
	public static void setup_mongo() throws UnknownHostException, IOException{
		
		String proxyHost = System.getenv("http.proxyHost");
		String proxyPort = System.getenv("http.proxyPort");
		
		String proxy = System.getenv("http_proxy");
		System.out.println("Proxy URL : " + proxy);
		if(proxy != null){
			if(proxyHost == null && proxyPort == null){
				URL proxyurl = new URL(proxy);
			   proxyHost = proxyurl.getHost();
			   proxyPort = String.valueOf(proxyurl.getPort());
			}  
		}
		
		MongodStarter starter ;
		
		System.out.println("Proxy Host : " + proxyHost);
		System.out.println("Proxy Port : " + proxyPort);
		if (proxyHost != null && proxyPort != null) {
			IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder().defaults(Command.MongoD)
					.artifactStore(
							new ArtifactStoreBuilder().defaults(Command.MongoD)
									.download(
											new DownloadConfigBuilder()
													.defaultsForCommand(Command.MongoD)
													.proxyFactory(
															new HttpProxyFactory(
																	proxyHost,
																	Integer.parseInt(proxyPort)))
													.build()).build()).build();
			 starter = MongodStarter.getInstance(runtimeConfig);
		} else {
			 starter = MongodStarter.getDefaultInstance();
		}
			                                        		
		
		IMongodConfig mongodConfig = new MongodConfigBuilder()
				.version(Version.Main.PRODUCTION)
				.net(new Net(mongoport, Network.localhostIsIPv6())).build();

		MongodExecutable mongodExecutable = null;

		mongodExecutable = starter.prepare(mongodConfig);
		mongod = mongodExecutable.start();
		
	}

	private void replace_values(String templatefile,
			Map<String, String> replacekeys, String newfilename)
			throws IOException {
		// we need to store all the lines
		List<String> lines = new ArrayList<String>();
		// first, read the file and store the changes
		FileReader reader = new FileReader(templatefile);
		BufferedReader in = new BufferedReader(reader);
		String line = in.readLine();
		try{
			while (line != null) {
				for (String key : replacekeys.keySet()) {
					if (line.contains(key)) {
						String sValue = line.replace(key, replacekeys.get(key));
						line = sValue;
					}
				}
				lines.add(line);
				line = in.readLine();
			}
	
			in.close();
			PrintWriter out = new PrintWriter(
					"src/test/java/com/ebay/jetstream/dynamicconfig/" + newfilename);
			for (String l : lines)
				out.println(l);
			out.close();
		}finally{
			if(in != null)
				in.close();
			if(reader != null)
				reader.close();
		}

	}

	private void set_MONGO_HOME(String host, int port) {
		System.setProperty("MONGO_HOME", "mongo://localhost:"+ port + "/jetstream");
	}
	
	private static void unset_MONGO_HOME() {
		System.clearProperty("MONGO_HOME");
	}
	
	

	@Before
	public void setup() throws Exception {

		template_replace_map.put("{zkport}", zkport);
		template_replace_map.put("{retrycount}", "5");
		template_replace_map.put("{sessionTimeoutInMillis}", "4000000");
		template_replace_map.put("{zkcontext1}", "zk");
		template_replace_map.put("{netty2}", "rtd");
		template_replace_map.put("{cnxnpooolsz}", "1");
		template_replace_map.put("{compression}", "false");
		

		replace_values(TEMPLATE_FILE, template_replace_map, "file/initial.xml");

		set_MONGO_HOME("localhost", mongoport);
		RootConfiguration.setConfigurationRoot(CONFIG_ROOT);
		RootConfiguration config = new RootConfiguration(
				new ApplicationInformation(appname, version));
		config.start();
		Thread.sleep(3000);

	}
	
	@Ignore
	public void change_with_scope() throws Exception {
		
		Map<String, String> zkretry = new HashMap<String, String>();
		zkretry.putAll(template_replace_map);
		zkretry.put("{retrycount}", "3");
		zkretry.put("{sessionTimeoutInMillis}", "5000000");
		replace_values(TEMPLATE_FILE, zkretry, "zk_retrycount_change.xml");
		
		String localhostname = java.net.InetAddress.getLocalHost().getHostName();
		
		upload_bean(FILE_PATH + "zk_retrycount_change.xml",
				"MessageServiceProperties", "local:" + localhostname , true);

		Thread.sleep(3000);
		ZooKeeperTransportConfig newconfig = (ZooKeeperTransportConfig )MessageService.getInstance().getMessageServiceProperties().getTransport("zookeeper");
		assertEquals(3, newconfig.getRetrycount());
		assertEquals(5000000, newconfig.getSessionTimeoutInMillis());

	}

	@Ignore
	public void zkconfigchange() throws Exception {

		Map<String, String> zkretry = new HashMap<String, String>();
		zkretry.putAll(template_replace_map);
		zkretry.put("{retrycount}", "4");
		zkretry.put("{sessionTimeoutInMillis}", "5000000");
		replace_values(TEMPLATE_FILE, zkretry, "zk_retrycount_change.xml");
		upload_bean(FILE_PATH + "zk_retrycount_change.xml",
				"MessageServiceProperties", "global",  true);

		Thread.sleep(3000);
		ZooKeeperTransportConfig newconfig = (ZooKeeperTransportConfig )MessageService.getInstance().getMessageServiceProperties().getTransport("zookeeper");
		assertEquals(4, newconfig.getRetrycount());
		assertEquals(5000000, newconfig.getSessionTimeoutInMillis());

	}

	@Ignore
	public void nettycontextchange() throws Exception {
		
		Map<String, String> nettychange = new HashMap<String, String>();
		nettychange.putAll(template_replace_map);
		nettychange.put("{cnxnpooolsz}", "3");
		nettychange.put("{compression}", "true");
		replace_values(TEMPLATE_FILE, nettychange, "netty_change.xml");
		upload_bean(FILE_PATH + "netty_change.xml",
				"MessageServiceProperties", "global" , true);

		Thread.sleep(3000);
		NettyTransportConfig newconfig = (NettyTransportConfig )MessageService.getInstance().getMessageServiceProperties().getTransport("netty2");
		assertEquals(3, newconfig.getConnectionPoolSz());
		assertTrue(newconfig.isEnableCompression());

	}

	

	private void netty_schedulerchange() {

	}

	private void netty_enable_disable_batching() {

	}

	private void upload_bean(String filename, String beanName, String scope, boolean publish) throws Exception {
		String[] configarr = new String[7];
		configarr[0] = "-app=" + appname;
		configarr[1] = "-version=" + version;
		configarr[2] = "-user=raji";
		configarr[3] = "-scope=" + scope;
		configarr[4] = "-beandefxml=" + filename;
		configarr[5] = "-beanid=" + beanName;
		configarr[6] = "-publish=" + publish;
		ConfigUploaderToMongo configuploader = new ConfigUploaderToMongo(
				configarr);
		configuploader.upload();
	}

	@AfterClass
	public static  void teardown() throws Exception {
		System.out.println("Tearing Down");
		mongod.stop();
		unset_MONGO_HOME();
	}

	@Ignore
	public void testupload() throws Exception {

		// upload_bean("MessageServiceProperties", );

		collection = db.getCollection("jetstream");
		DBCursor cursorDoc = collection.find();
		BasicDBObject beanobject = new BasicDBObject();

		while (cursorDoc.hasNext()) {
			beanobject = (BasicDBObject) cursorDoc.next();
			System.out.println(beanobject);
		}

	}

}
