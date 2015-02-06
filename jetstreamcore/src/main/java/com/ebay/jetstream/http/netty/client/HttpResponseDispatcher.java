/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.client;

import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Timer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.ebay.jetstream.common.NameableThreadFactory;
import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.util.TimeSlotHashMap;

public class HttpResponseDispatcher implements TimeSlotHashMap.Listener {

    final static int SIX_TIME_SLOTS = 6;
    private TimeSlotHashMap<ResponseFuture> m_responseFutureRegistry = null;
    Timer m_timer;
    private ThreadPoolExecutor m_servletExecutor;
    private BlockingQueue<Runnable> m_workQueue;
    private LongCounter m_dropCounter = new LongCounter();
    private LongCounter m_responseCounter = new LongCounter();
    private LongCounter m_queueFullCounter = new LongCounter();
    private LongCounter m_timeoutCounter = new LongCounter();
    
    public long getQueueFullCounter() {
        return m_queueFullCounter.get();
    }
    
    public long getResponseCounter() {
        return m_responseCounter.get();
    }
    
    public long getDropCounter() {
        return m_dropCounter.get();
    }

    public long getTimeoutCounter() {
        return m_timeoutCounter.get();
    }

    
    public HttpResponseDispatcher(Timer timer, int workQueueSz, int threadPoolSz) {
        m_timer = timer;
        m_responseFutureRegistry = new TimeSlotHashMap<ResponseFuture>(m_timer, SIX_TIME_SLOTS, this);
        m_workQueue = new ArrayBlockingQueue<Runnable>(workQueueSz, true);
        m_servletExecutor = new ThreadPoolExecutor(threadPoolSz, threadPoolSz, 1, TimeUnit.SECONDS, m_workQueue,
                new NameableThreadFactory("Jetstream-HttpResponseDispatcher"));
        m_servletExecutor.prestartAllCoreThreads();
    }

    public void add(RequestId id, ResponseFuture future) {

        m_responseFutureRegistry.put(id.getKey(), future);

    }

    public void dispatch(String requestid, HttpResponse response) {

        // this method is being called from IO thread - we will dispatch this in
        // a background thread

        ResponseFuture future = (ResponseFuture) m_responseFutureRegistry.remove(TimeSlotHashMap.Key.newKey(requestid));
        m_responseCounter.increment();
        if (future != null) {
            if (HttpResponseStatus.OK.equals(response.getStatus()))
                future.setSuccess();
            else
                future.setFailure();
            HttpResponseDispatchRequest workRequest = new HttpResponseDispatchRequest(future, response);
            if (!m_workQueue.offer(workRequest))
                m_queueFullCounter.increment();
        } else {
            m_dropCounter.increment();
        }

    }

    @Override
    public void onTimeout(Object obj) {

        ((ResponseFuture) obj).setTimedout();
        m_timeoutCounter.increment();
        HttpResponseDispatchRequest workRequest = new HttpResponseDispatchRequest(((ResponseFuture) obj), null);
        if (!m_workQueue.offer(workRequest))
            m_queueFullCounter.increment();

    }

    public ResponseFuture remove(String reqid) {

        return m_responseFutureRegistry.remove(TimeSlotHashMap.Key.newKey(reqid));

    }

    public void shutDown() {
        m_responseFutureRegistry.cancel();
        m_servletExecutor.shutdown();
    }

}
