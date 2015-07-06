/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;

import com.ebay.jetstream.common.ShutDownable;
import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.spring.beans.factory.BeanChangeAware;

/**
 * @author weifang
 * 
 */
public class HdfsClient extends AbstractNamedBean implements InitializingBean,
		ShutDownable, ApplicationListener, BeanChangeAware {
	private static Logger LOGGER = Logger.getLogger(HdfsClient.class.getName());

	// injected
	private HdfsClientConfig config;

	// internal
	private Configuration hdpConf;
	private FileSystem fs;

	public void setConfig(HdfsClientConfig config) {
		this.config = config;
	}

	@Override
	public void onApplicationEvent(ApplicationEvent event) {
		// TODO Hot deploy
	}

	@Override
	public int getPendingEvents() {
		return 0;
	}

	@Override
	public void shutDown() {
		try {
			fs.close();
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.toString(), e);
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		initHdfs();
	}

	protected void initHdfs() {
		hdpConf = new Configuration();
		final String hdfsUrl = config.getHdfsUrl();
		UserGroupInformation ugi = UserGroupInformation.createRemoteUser(config
				.getUser());

		try {
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				@Override
				public Void run() throws Exception {
					hdpConf.set("hadoop.job.ugi", config.getUser());
					hdpConf.set("fs.defaultFS", hdfsUrl);
					if (hdfsUrl.startsWith("hdfs")) {
						for (Object keyObj : config.getHadoopProperties()
								.keySet()) {
							String key = (String) keyObj;
							hdpConf.set(key, config.getHadoopProperties()
									.getProperty(key));
						}
						fs = new DistributedFileSystem();
						fs.initialize(URI.create(hdfsUrl), hdpConf);
					} else {
						fs = FileSystem.get(hdpConf);
					}
					LOGGER.log(Level.INFO,
							"Connected to HDFS with the following properties: hdfsUrl "
									+ hdfsUrl);
					return null;
				}

			});
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "Error initializing HdfsClient. Error:"
					+ e);
		}
	}

	protected void closeHdfs() {
		if (fs != null) {
			try {
				fs.close();
			} catch (Exception e) {
				LOGGER.log(Level.SEVERE, "Error closing HdfsClient. Error:" + e);
			}
		}
	}

	public Configuration getHadoopConfig() {
		return hdpConf;
	}

	public OutputStream createFile(String path, boolean overwrite)
			throws IOException {
		Path hpath = new Path(URI.create(path));
		return fs.create(hpath, overwrite);
	}

	public boolean delete(String path, boolean recursive) throws IOException {
		Path hpath = new Path(URI.create(path));
		return fs.delete(hpath, recursive);
	}

	public void createFolder(String path) throws IOException {
		final Path hpath = new Path(URI.create(path));
		if (!fs.exists(hpath)) {
			fs.mkdirs(hpath);
		}
	}

	public boolean rename(String srcPath, String dstPath) throws IOException {
		final Path src = new Path(URI.create(srcPath));
		final Path dst = new Path(URI.create(dstPath));
		return fs.rename(src, dst);
	}

	public boolean exist(String srcPath) throws IOException {
		final Path src = new Path(URI.create(srcPath));
		return fs.exists(src);
	}
}
