/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.notification;

import java.util.Properties;

import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.config.ContextBeanChangedEvent;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.spring.beans.factory.BeanChangeAware;
import com.ebay.jetstream.xmlser.XSerializable;


public class MailAlertListener extends AbstractNamedBean 
  implements BeanChangeAware,ApplicationListener, AlertListener, XSerializable, InitializingBean{
	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.notification.MailSender");
	private MailConfiguration m_config;
	Properties properties;
	
	@Override
	public boolean sendAlert(String source, String msg, AlertStrength level) {
		if (msg == null || level == null) {
			LOGGER.warn( "alert msg is null or alert strength is null, ignored");
			return false;
		}

		if (level == AlertStrength.GREEN || level == AlertStrength.YELLOW) {
			setStatus(source, msg, level);
			return true;
		} else {
			return setStatus(source, msg, level, true);
		}
	}
	
	
	public void sendMail(String componentName, String status, AlertStrength level, long timeMs) { 
      Session session = Session.getInstance(properties, null); 
      try{
          MimeMessage message = new MimeMessage(session);
          message.addHeader("Content-type", "text/HTML; charset=UTF-8");
          
          message.setFrom(new InternetAddress(m_config.getSendFrom()));
          
          message.setRecipients(Message.RecipientType.TO,
        		  InternetAddress.parse(m_config.getAlertList(), false));
          
		  StringBuffer sb = new StringBuffer();
		  sb.append("component name=").append(componentName).append(":")
		          .append(new java.util.Date(timeMs)).append(", msg=").append(status);
		  message.setText(sb.toString(), "UTF-8");
		  
		  StringBuffer subject = new StringBuffer();
		  subject.append("component name=").append(componentName).append("has a :")
		          .append(level +"alert for you to look into.");
		  message.setSubject(subject.toString(), "UTF-8");

          Transport.send(message);
       }catch (Throwable mex) {
          LOGGER.warn( mex.getLocalizedMessage());
       }
	}
	
	public void setStatus(String component, String status, AlertStrength level) {
		setStatus(component, status, level, false);
	}
	
	public boolean setStatus(String component, String status, AlertStrength level, boolean sendAlertOnThis) {
		if (component == null || status == null) {
			LOGGER.warn( "component or status is null, ignored");
			return false;
		}

		if (sendAlertOnThis) {
			StatusApplicationEvent e = new StatusApplicationEvent(component, status);
			
			LOGGER.warn( "got status for name=" + component + 
					         ", newValue=" + status + 
					          ", sendAlert=" + sendAlertOnThis);
			
			sendMail(component, status, level, e.getTimestamp());
		}
		return true;
	}
	
	private void setProperty(){
		properties = new Properties();
		properties.setProperty("mail.smtp.host", m_config.getMailServer());
	}

	public void setMailConfiguration(MailConfiguration config) {
		if (config != null) {
			m_config = config;
		}
		setProperty();
	}


	@Override
	public void afterPropertiesSet() throws Exception {
		Management.removeBeanOrFolder(getBeanName(), this);
		Management.addBean(getBeanName(), this);
		
		if (m_config == null) {
			LOGGER.warn( " Bean SMTPMailConfiguration with properties missing and should be added");
			throw new Exception("null SMTPConguration. Make sure SMTPMailsender depends on SMTPConfiguration");
		}
	}


	@Override
	public void onApplicationEvent(ApplicationEvent event) {
		if (event instanceof ContextBeanChangedEvent) {
			ContextBeanChangedEvent bcInfo = (ContextBeanChangedEvent)event;
		
			if (bcInfo.getBeanName().equals(m_config.getBeanName())) {
				LOGGER.info("Received dynamic notification to apply new config", getBeanName());
				
				Object objChangeBean = bcInfo.getChangedBean();
				if (objChangeBean instanceof MailConfiguration) {
					m_config = (MailConfiguration)objChangeBean;
				}
			}
		}		
	}
}
