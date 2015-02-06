/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.notification;
/***
 * Applications can implement this interface to send alerts from Jetstream framework. 
 * Jetstream components/beans needs to reference the implementation so the applications can have
 * customized alerting mechanism.
 * 
 *	<bean id="DalInit" class="com.ebay.jetstream.dal.DalInitializer" lazy-init="true" depends-on="componentStatusHarvester">
 *		<property name="resourceRoot" value="${JETSTREAM_HOME}/JetstreamConf/dalconfig/${COS}" />
 *		<property name="alertListener" ref="componentStatusHarvester" />
 *	</bean>
 *
 *  In this example, we use "componentStatusHarvester", which is the default alert listener. Applications just
 *  need to replace it with own implementation of this interface here so DalInit bean will send an alert based on
 *  Applications own requirements.
 *  
 *  
 * @author gjin
 *
 */
public interface AlertListener {
	
	/**** the strength is in the order of RED, ORANGE, YELLOW, GREEN with RED as the highest
	 *    Implementations treat these values at own discretions. For example:
	 **   Certain implementation can treat YELLOW, GREEN for notification only and no alert being sent out
	 ***/
	public enum AlertStrength { RED, ORANGE, YELLOW, GREEN};
	
	
	/**
	 * an Alert (TEC alert for example) may be sent. However, the implementations can
	 * ignore this alert request if the implementation's own config saying disabling the alert
	 * 
	 * @param source the component/bean sending the alert
	 * @param msg, the alert msg
	 * @param strength, alert level. Implementations can have own interpretation of it
	 * 
	 * @return true if the msg was sent successfully.
	 */
	boolean sendAlert(String source, String msg, AlertStrength strength);
}
