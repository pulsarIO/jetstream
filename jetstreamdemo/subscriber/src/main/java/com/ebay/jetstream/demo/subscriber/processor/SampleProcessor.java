/*******************************************************************************
 * Copyright 2012-2015 eBay Software Foundation
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.ebay.jetstream.demo.subscriber.processor;

import org.springframework.context.ApplicationEvent;
import org.springframework.jmx.export.annotation.ManagedResource;


import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.support.AbstractEventProcessor;
import com.ebay.jetstream.management.Management;

@ManagedResource(objectName = "Event/Processor", description = "SampleProcessor")
public class SampleProcessor extends AbstractEventProcessor {
    private JetstreamEvent lastEvent;
    
    @Override
    public void afterPropertiesSet() throws Exception {
        Management.addBean(getBeanName(), this);
    }

    public JetstreamEvent getLastEvent() {
        return lastEvent;
    }

    @Override
    public int getPendingEvents() {
        return 0;
    }

    @Override
    public void pause() {
        
    }

    @Override
    protected void processApplicationEvent(ApplicationEvent event) {
        
    }

    @Override
    public void resume() {
        
    }

    @Override
    public void sendEvent(JetstreamEvent event) throws EventException {
        lastEvent = event;
        super.incrementEventRecievedCounter();
        System.out.println(event);
        super.fireSendEvent(event);
        super.incrementEventSentCounter();
    }

    public void setLastEvent(JetstreamEvent lastEvent) {
        this.lastEvent = lastEvent;
    }

    @Override
    public void shutDown() {
        
    }

}
