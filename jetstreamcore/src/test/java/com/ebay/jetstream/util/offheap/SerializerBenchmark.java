/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.ebay.jetstream.util.offheap.serializer.DefaultSerializerFactory;


public class SerializerBenchmark {
    public static void main(String[] args) {
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("g", "898b96a81430a5f164564456fb9dc5e8");
        event.put("timestamp", System.currentTimeMillis());
        event.put("Agent", "Opera/9.80 (Windows NT 6.1; WOW64; MRA 6.0 (build 6068)) Presto/2.12.388 Version/12.16");
        event.put("RemoteIP", "27.115.118.82");
        event.put("cid", "1");
        event.put("Referer", "RefererV");
        event.put("p", "123");
        event.put("sessionTTL", 60000);
        event.put("u", "123");
        event.put("app", "1");
        event.put("rdt", 0);
        event.put("iframe", 0);
        event.put("rv", Boolean.TRUE);
        event.put("Payload", "/roverimp/0/0/14?rvrhostname=rover.ebay.com&rvrsite=0&trknvpsvc=<a>es=0&ec=4</a>&lv=udid=60397c64141df9693622f2f001652776&ai=1462&mav=3.2.0&site=0&ou=nY+sHZ2PrBmdj6wVnY+sEZ2PrA2dj6AFmYumDJSLqQWdj6x9nY+seQ**&tz=7.00&curprice=24.99&dn=iPhone5_1&srrank=17&uc=VN&tzname=Asia/Saigon&osv=7.0.4&idfa=4A7BE614-E5AA-476A-ADA2-477DF734546D&un=ksor_phong1977&storeavlbl=0&res=1136X640&itm=221353405632&prefl=vi-VN&flgs=AAADIA**&ids=MP=ksor_phong1977&bc=0&mrollp=92.41&bi=0.5&nw=0&nofp=5&mlocset=0&mqt=M3,M4&saa=1&si=1&sn=ballstatebaseball&leaf=14003&shipsiteid=0&c=40&qtya=1&ort=P&fdp=100.00&mnt=wifi&carrier=Viettel&qtys=0&tr=588231&ttp=Page&mtsts=2014-01-12T20:58:55.299&imp=2052300");
        
        OffHeapSerializer<Map> s = DefaultSerializerFactory.getInstance().createKryoSerializer();
        run(event, s);
        
        OffHeapSerializer<Map> s2 = DefaultSerializerFactory.getInstance().createObjectSerializer();
        run(event, s2);
    }

    private static void run(Map<String, Object> event, OffHeapSerializer<Map> s) {
        int count = 1000000;
        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            ByteBuffer buffer = s.serialize(event);
            Map<String, Object> o = s.deserialize(buffer, buffer.position(), buffer.limit() - buffer.position());
        }
        System.out.println(s.getClass() + ":" + ((System.nanoTime() - start) / count));
    }
}

