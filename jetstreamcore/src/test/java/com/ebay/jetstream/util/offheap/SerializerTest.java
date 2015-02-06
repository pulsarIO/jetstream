/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

import junit.framework.Assert;

import org.junit.Test;

import com.ebay.jetstream.util.offheap.serializer.DefaultSerializerFactory;
import com.ebay.jetstream.util.offheap.serializer.util.PrimitiveTLVEncoder;

public class SerializerTest {
    private static class Foo implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private String name;
        private int age;
        public String getName() {
            return name;
        }
        public void setName(String name) {
            this.name = name;
        }
        public int getAge() {
            return age;
        }
        public void setAge(int age) {
            this.age = age;
        }
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + age;
            result = prime * result + ((name == null) ? 0 : name.hashCode());
            return result;
        }
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Foo other = (Foo) obj;
            if (age != other.age)
                return false;
            if (name == null) {
                if (other.name != null)
                    return false;
            } else if (!name.equals(other.name))
                return false;
            return true;
        }
        
    }
    
    @Test
    public void testKryo() {
        OffHeapSerializer<Foo> s = DefaultSerializerFactory.getInstance().createKryoSerializer();
        Foo foo = new Foo();
        foo.setAge(1);
        foo.setName("X");
        
        runTest(s, foo);
    }
    
    @Test
    public void testPOJO() {
        OffHeapSerializer<Foo> s = DefaultSerializerFactory.getInstance().createObjectSerializer();
        Foo foo = new Foo();
        foo.setAge(1);
        foo.setName("X");
        
        runTest(s, foo);
    }
    @Test
    public void testPrimitiveTLVEncoder() {
        PrimitiveTLVEncoder s = new PrimitiveTLVEncoder();
        
        testPrimitive(s, null);
        testPrimitive(s, 1);
        testPrimitive(s, -2);
        testPrimitive(s, 0);
        testPrimitive(s, -1);
        testPrimitive(s, 1>>7);
        testPrimitive(s, 1>>7 + 1);
        testPrimitive(s, 1>>14);
        testPrimitive(s, 1>>14 + 1);
        testPrimitive(s, 1>>21);
        testPrimitive(s, 1>>21 + 1);
        testPrimitive(s, 1>>28);
        testPrimitive(s, 1>>28 + 1);
        testPrimitive(s, 1>>29);
        testPrimitive(s, Integer.MAX_VALUE);
        testPrimitive(s, Integer.MIN_VALUE);
        testPrimitive(s, true);
        testPrimitive(s, false);
        testPrimitive(s, 1L);
        testPrimitive(s, -2L);
        testPrimitive(s, 0L);
        testPrimitive(s, -1L);
        testPrimitive(s, (long) 1>>7);
        testPrimitive(s, 1>>7 + 1L);
        testPrimitive(s, (long) 1>>14);
        testPrimitive(s, 1>>14 + 1L);
        testPrimitive(s, (long) 1>>21);
        testPrimitive(s, 1>>21 + 1L);
        testPrimitive(s, (long) 1>>28);
        testPrimitive(s, 1>>28 + 1L);
        testPrimitive(s, (long) 1>>29);
        testPrimitive(s, (long) 1>>35);
        testPrimitive(s, 1>>35 + 1L);
        testPrimitive(s, (long) 1>>42);
        testPrimitive(s, 1>>42 + 1L);
        testPrimitive(s, (long) 1>>49);
        testPrimitive(s, 1>>49 + 1L);
        testPrimitive(s, (long) 1>>56);
        testPrimitive(s, 1>>56 + 1L);
        testPrimitive(s, Long.MAX_VALUE);
        testPrimitive(s, Long.MIN_VALUE);
        testPrimitive(s, 7L);
        testPrimitive(s, 'c');
        testPrimitive(s, Double.MAX_VALUE);
        testPrimitive(s, Double.MIN_VALUE);
        testPrimitive(s, 1.0);
        testPrimitive(s, Float.MAX_VALUE);
        testPrimitive(s, Float.MIN_VALUE);
        testPrimitive(s, 4.5f);
        testPrimitive(s, Byte.MAX_VALUE);
        testPrimitive(s, Byte.MIN_VALUE);
        testPrimitive(s, (byte) 1);
        testPrimitive(s, Short.MAX_VALUE);
        testPrimitive(s, Short.MIN_VALUE);
        testPrimitive(s, (short) 1);
        testPrimitive(s, "sddf");
        testPrimitive(s, "");
        testPrimitive(s, new Date());
        testPrimitive(s, new ArrayList<String>() {{ add("xx"); add("yy");}});
        testPrimitive(s, new HashSet<String>() {{ add("xx"); add("yy");}});
        testPrimitive(s, new HashMap<String, String>() {{ put("xx", "zz");put("yy", "zz"); }});
        testPrimitive(s, new HashMap<String, String>() {{ put("xx", "zz");put("yy", "zz"); }});

    }
    
    private void testPrimitive(PrimitiveTLVEncoder s, Object v) {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        s.encode(v, buffer);
        buffer.flip();
        Object object = s.decode(buffer);
        Assert.assertEquals(v, object);
    }
    
    @Test
    public void testObjectSerializer() {
        OffHeapSerializer<String> s = DefaultSerializerFactory.getInstance().createObjectSerializer();
        runTest(s, null);
        runTest(s, "\u1234");
        runTest(s, "a");
        runTest(s, "a\u1234b\u1234");
        runTest(s, "\u1234b\u1234c");
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < 1024; i++) {
            b.append("" + i);
            runTest(s, b.toString());
        }
        for (int i = 0; i < 1024; i++) {
            b.append("" + i);
            b.append("\u1234");
            runTest(s, b.toString());
        }
    }

    @Test
    public void testLongSerializer() {
        OffHeapSerializer<Long> s = DefaultSerializerFactory.getInstance().getLongSerializer();
        runTest(s, 0L);
        runTest(s, -1L);
        runTest(s, 1L);
        runTest(s, Long.MAX_VALUE);
        runTest(s, Long.MIN_VALUE);
    }

    @Test
    public void testIntSerializer() {
        OffHeapSerializer<Integer> s = DefaultSerializerFactory.getInstance().getIntSerializer();
        runTest(s, 0);
        runTest(s, -1);
        runTest(s, 1);
        runTest(s, Integer.MAX_VALUE);
        runTest(s, Integer.MIN_VALUE);
    }

    @Test
    public void testASCIIStringSerializer() {
        OffHeapSerializer<String> s = DefaultSerializerFactory.getInstance().getStringSerializer();
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < 1024; i++) {
            b.append("" + i % 10);
            runTest(s, b.toString());
        }
    }

    @Test
    public void testUTF8StringSerializer() {
        OffHeapSerializer<String> s = DefaultSerializerFactory.getInstance().getStringSerializer();
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < 1024; i++) {
            b.append("\u1234");
            runTest(s, b.toString());
        }
    }
    @Test
    public void testStringSerializer() {
        OffHeapSerializer<String> s = DefaultSerializerFactory.getInstance().getStringSerializer();
        runTest(s, null);
        runTest(s, "\u1234");
        runTest(s, "a");
        runTest(s, "a\u1234b\u1234");
        runTest(s, "\u1234b\u1234c");
    }

    private <T> void runTest(OffHeapSerializer<T> s, T i) {
        ByteBuffer buffer = s.serialize(i);
        T v = s.deserialize(buffer, buffer.position(), buffer.limit() - buffer.position());
        
        Assert.assertEquals(v, i);
    }
}
