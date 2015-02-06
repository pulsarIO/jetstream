/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.notification;

import org.junit.Test;

public class MailTest {
	MailAlertListener mailing;
	MailConfiguration mailConfig;
	
	@Test
	public void SMTPChannelTest(){
		mailConfig = new MailConfiguration();
		mailConfig.setAlertList("pj1@gmail.com");
		mailConfig.setSendFrom("pj1@gmail.com");
		mailConfig.setAlertSeverity("WARNING");
		mailConfig.setMailServer("smtp.gmail.com");
		mailConfig.setBeanName("SMTPMailConfiguration");

		mailing = new MailAlertListener();
		mailing.setMailConfiguration(mailConfig);
		
		mailing.sendAlert("SMTPTest", "Testing SMTPMessage", AlertListener.AlertStrength.RED);
	}
}
