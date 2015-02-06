/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.xmlser.XSerializable;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

@ManagedResource(objectName = "Kafka", description = "Kafka metrics")
public class KafkaMetrics extends AbstractNamedBean implements InitializingBean {
    public static class MeterPOJO implements XSerializable {
        private final long count;
        private final double fifteenMinuteRate;
        private final double fiveMinuteRate;
        private final double meanRate;
        private final String rateUnit;
        private final double oneMinuteRate;
        
        public MeterPOJO(Meter m) {
            count = m.count();
            fifteenMinuteRate = m.fifteenMinuteRate();
            fiveMinuteRate = m.fiveMinuteRate();
            meanRate = m.meanRate();
            rateUnit = m.rateUnit().toString();
            oneMinuteRate = m.oneMinuteRate();
        }

        public long getCount() {
            return count;
        }

        public double getFifteenMinuteRate() {
            return fifteenMinuteRate;
        }

        public double getFiveMinuteRate() {
            return fiveMinuteRate;
        }

        public double getMeanRate() {
            return meanRate;
        }

        public String getRateUnit() {
            return rateUnit;
        }

        public double getOneMinuteRate() {
            return oneMinuteRate;
        }
    }
    
    @Override
    public void afterPropertiesSet() throws Exception {
        Management.addBean(getBeanName(), this);
    }
    
    @ManagedAttribute
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<String, Object>();
        
        MetricsRegistry registry = Metrics.defaultRegistry();
        for (Entry<MetricName, Metric> e : registry.allMetrics().entrySet()) {
            MetricName name = e.getKey();
            Metric metric = e.getValue();
            
            if (metric instanceof Meter) {
                Meter m = (Meter) metric;
                stats.put(name.toString(), new MeterPOJO(m));
            } else if (metric instanceof Gauge) {
                Gauge<?> g = (Gauge<?>) metric;
                stats.put(name.toString(), g.value());
            }
        }
        
        return stats;
    }
}
