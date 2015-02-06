/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import java.util.Properties;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * This is the config bean for the kafka producer.
 * 
 * Below is a configuration example.
 * <pre>
 *  	<bean id="producerConfig" class="com.ebay.jetstream.kafka.KafkaProducerConfig">    
 *        <property name="props">
 * 			<props>
 * 				<prop key="broker.list">10.9.241.144:9092,10.9.252.125:9092,10.9.255.14:9092</prop>                                     
 * 				<prop key="request.required.acks">0</prop>                           
 * 				<prop key="producer.type">sync</prop>                                   
 * 				<prop key="serializer.class">kafka.serializer.StringEncoder</prop>                                
 * 				<prop key="key.serializer.class">kafka.serializer.StringEncoder</prop>                            
 * 				<prop key="partitioner.class">kafka.producer.DefaultPartitioner</prop>                               
 * 				<prop key="compression.codec">none</prop>                               
 * 				<prop key="compressed.topics">null</prop>                               
 * 				<prop key="message.send.max.retries">3</prop>                        
 * 				<prop key="retry.backoff.ms">100</prop>                                
 * 				<prop key="topic.metadata.refresh.interval.ms">600000</prop>              
 * 				<prop key="queue.buffering.max.ms">5000</prop>                          
 * 				<prop key="queue.buffering.max.messages">10000</prop>                    
 * 				<prop key="queue.enqueue.timeout.ms">-1</prop>                        
 * 				<prop key="batch.num.messages">200</prop>                              
 * 				<prop key="send.buffer.bytes">102400</prop>                               
 * 				<prop key="client.id"></prop>                                       
 * 				<prop key="request.timeout.ms">1500</prop>                              
 * 			</props>
 * 		</property>
 *    	</bean>
 * </pre>
 * 
 * @author xingwang
 * 
 */
public class KafkaProducerConfig extends AbstractNamedBean implements XSerializable {
    private boolean enabled;
    private int poolSize = 1;
    private String timestampKey;
	private Properties props = new Properties();

    public boolean getEnabled() {
        return enabled;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public Properties getProps() {
		return props;
	}

    public String getTimestampKey() {
        return timestampKey;
    }

	public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

	public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    public void setProps(Properties props) {
		this.props = props;
	}

    public void setTimestampKey(String timestampKey) {
        this.timestampKey = timestampKey;
    }
}
