/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.Test;

import com.ebay.jetstream.event.BatchEventSink;
import com.ebay.jetstream.event.EventSink;
import com.ebay.jetstream.event.support.AbstractBatchEventProcessor;

public class IkcValidationTest {

	@Test
	public void testValidationNonBatchLinked() {
		// non-batch event sink linked
		TestInboundKafkaChannel ikc = new TestInboundKafkaChannel();
		EventSink sink = mock(EventSink.class);
		ikc.addEventSink(sink);
		boolean thrown = false;
		try {
			invokeValidation(ikc);
		} catch (Exception e) {
			assertTrue(e.getCause() instanceof RuntimeException);
			thrown = true;
		}
		assertTrue(thrown);
	}

	@Test
	public void testValidationNonProcessorLinked() {
		// non-batchprocessor linked
		TestInboundKafkaChannel ikc = new TestInboundKafkaChannel();
		BatchEventSink batchSink = mock(BatchEventSink.class);
		ikc.addBatchEventSink(batchSink);
		boolean thrown = false;
		try {
			invokeValidation(ikc);
		} catch (Exception e) {
			assertTrue(e.getCause() instanceof RuntimeException);
			thrown = true;
		}
		assertTrue(thrown);
	}

	@Test
	public void testValidationMoreThanOneLinked() {
		// more than one batchprocessor linked
		TestInboundKafkaChannel ikc = new TestInboundKafkaChannel();
		AbstractBatchEventProcessor batchProcessor1 = mock(AbstractBatchEventProcessor.class);
		AbstractBatchEventProcessor batchProcessor2 = mock(AbstractBatchEventProcessor.class);
		ikc.addBatchEventSink(batchProcessor1);
		ikc.addBatchEventSink(batchProcessor2);
		boolean thrown = false;
		try {
			invokeValidation(ikc);
		} catch (Exception e) {
			assertTrue(e.getCause() instanceof RuntimeException);
			thrown = true;
		}
		assertTrue(thrown);
	}

	@Test
	public void testValidationYes() {
		// validate, one batch processor linked
		TestInboundKafkaChannel ikc = new TestInboundKafkaChannel();
		KafkaConsumerConfig config = new KafkaConsumerConfig();
		config.setPoolSize(2);
		ikc.setConfig(config);
		
		AbstractBatchEventProcessor batchProcessor = mock(AbstractBatchEventProcessor.class);
		ikc.addBatchEventSink(batchProcessor);

		boolean thrown = false;
		try {
			invokeValidation(ikc);
		} catch (Exception e) {
			e.printStackTrace();
			thrown = true;
		}
		assertFalse(thrown);
	}

	@Test
	public void testValidationPoolSizeNeg() {
		// poolSize <= 0
		TestInboundKafkaChannel ikc = new TestInboundKafkaChannel();
		KafkaConsumerConfig config = new KafkaConsumerConfig();
		config.setPoolSize(0);
		ikc.setConfig(config);
		AbstractBatchEventProcessor batchProcessor0 = mock(AbstractBatchEventProcessor.class);
		ikc.addBatchEventSink(batchProcessor0);
		boolean thrown = false;
		try {
			invokeValidation(ikc);
		} catch (Exception e) {
			assertTrue(e.getCause() instanceof RuntimeException);
			thrown = true;
		}
		assertTrue(thrown);
	}

	private void invokeValidation(InboundKafkaChannel ikc) throws Exception {
		ReflectFieldUtil.invokeMethod(InboundKafkaChannel.class, ikc,
				"validation", new Object[0]);
	}

}
