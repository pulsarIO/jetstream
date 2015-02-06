/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.notification;

import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.xmlser.XSerializable;

public class MailConfiguration extends AbstractNamedBean implements XSerializable{
	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.notification.MailConfiguration");

	private String alertList;
	private String sendFrom;
	private AlertSeverity alertSeverity = AlertSeverity.SEVERE;
    private String mailServer;
    
    public String getSendFrom() {
		return sendFrom;
	} 

	public void setSendFrom(String sendFrom) {
		this.sendFrom = sendFrom;
		LOGGER.warn( "send mail from: "+ this.sendFrom);
	}
    
    public String getMailServer() {
		return mailServer;
	} 

	public void setMailServer(String mailserver) {
		this.mailServer = mailserver;
		LOGGER.warn( "Mail host set to: "+ this.mailServer);
	}

	public void setAlertSeverity(String severity) {    	
    	if (severity == null || severity.toString().length() ==0) {
    		LOGGER.warn( "severity is null or empty, use the default=" + AlertSeverity.SEVERE);
    		return;
    	}
		this.alertSeverity = AlertSeverity.valueOf( severity);
   		LOGGER.warn( "MailConfiguration set severity=" + this.alertSeverity);
   	}
	
	public AlertSeverity getAlertSeverity(){
		return alertSeverity;
	}


    
    /*** should be comma-separated email list
     * @param emailListString: should be comma-separated. "xyz@ebay.com,abcd@ebay.com"
     */
    public void setAlertList(String emailListString) {
    	
    	if (emailListString == null || emailListString.length() ==0) {
    		LOGGER.error( "alert_list should not be null or empty. use the default=" + alertList);
    		return;
    	}
    	emailListString = emailListString.replaceAll(" ", "");
    	String[] emailList = emailListString.split(",");
    	StringBuffer validEmailStr = new StringBuffer();
    	for (int i=0; i < emailList.length; i++){
    		if(isValidEmailAddress(emailList[i]))
    			validEmailStr.append(emailList[i]+",");
    	}
    	alertList = validEmailStr.toString().substring(0, validEmailStr.length()-1);
   		LOGGER.warn( "MailConfiguration set alertList=" + alertList);
    }
    
	public static boolean isValidEmailAddress(String email) {
	    boolean result = true;
	    try {
	       InternetAddress emailAddr = new InternetAddress(email);
	       emailAddr.validate();
	    } catch (AddressException ex) {
	       result = false;
	    }
	    return result;
	}
    
	public String getAlertList(){
		return alertList;
	}
}
