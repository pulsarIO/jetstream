/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka.support;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.channel.kafka.KafkaMessageSerializer;
import com.google.common.base.Charsets;

public class JSONMessageSerializer implements KafkaMessageSerializer {
    private JsonFactory factory;
    private String keyName = com.ebay.jetstream.event.JetstreamReservedKeys.MessageAffinityKey.toString();
    private boolean filterOutNullValue = false;

    public void setFilterOutNullValue(boolean b) {
        filterOutNullValue = b;
    }
    
    private AtomicLong seq = new AtomicLong(0);

    public JSONMessageSerializer() {
        factory = new JsonFactory();
    }

    @Override
    public JetstreamEvent decode(byte[] key, byte[] message) {
        try {
            @SuppressWarnings("unchecked")
            HashMap<String,Object> result =
                    new ObjectMapper().readValue(factory.createJsonParser(message), HashMap.class);
            return new JetstreamEvent(result);
        } catch (JsonParseException e) {
            throw new IllegalArgumentException(
                    "Fail to parse the bytes", e);
        } catch (JsonMappingException e) {
            throw new IllegalArgumentException(
                    "Fail to parse the bytes", e);
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Fail to parse the bytes", e);
        }
    }
    
    @Override
    public byte[] encodeKey(JetstreamEvent event) {
        if (keyName == null) {
            return genRandomKey();
        }
        Object key = event.get(keyName);
        if (key == null) {
            return genRandomKey();
        }
        if (key instanceof byte[]) {
            return (byte[]) key;
        } else if (key instanceof String) {
            return ((String) key).getBytes(Charsets.UTF_8);
        }
        throw new IllegalArgumentException("The key field should be byte[] or String.");
    }

    private void encodeMap(Map<String, Object> event, JsonGenerator generator)
            throws IOException, JsonGenerationException,
            JsonProcessingException {
        generator.writeStartObject();
        for (Map.Entry<String, Object> x : event.entrySet()) {
            generateEntry(generator, x);
        }
        generator.writeEndObject();
    }
    
    @Override
    public byte[] encodeMessage(JetstreamEvent event) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JsonGenerator generator;
        try {
            generator = factory.createJsonGenerator(out, JsonEncoding.UTF8);
        } catch (IOException e) {
            throw new IllegalStateException("Can not create json generator", e);
        }

        try {
            generator.writeStartObject();
            for (Map.Entry<String, Object> x : event.entrySet()) {
                generateEntry(generator, x);
            }
            generator.writeEndObject();
            generator.close();
        } catch (JsonGenerationException e) {
            throw new IllegalArgumentException(
                    "Fail to convert the object to json format", e);
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Fail to convert the object to json format", e);
        }
        return out.toByteArray();
    }

    @SuppressWarnings("unchecked")
    private void generateEntry(JsonGenerator generator,
            Map.Entry<String, Object> x) throws IOException,
            JsonGenerationException, JsonProcessingException {
        if (x.getValue() == null) {
            if (!filterOutNullValue) {
                generator.writeFieldName(x.getKey());
                generator.writeNull();
            }
            return;
        }

        generator.writeFieldName(x.getKey());
        if (Map.class.isAssignableFrom(x.getValue().getClass())) {
            encodeMap((Map<String, Object>) x.getValue(), generator);
        } else if (x.getValue().getClass().isArray()) {
            generator.writeStartArray();
            int length = Array.getLength(x.getValue());
            for (int i = 0; i < length; i++) {
                generator.writeObject(Array.get(x.getValue(), i));    
            }
            generator.writeEndArray();
        } else if (x.getValue() instanceof Collection) {
            generator.writeStartArray();
            Iterator iterator = ((Collection) (x.getValue())).iterator();
            while (iterator.hasNext()) {
                generator.writeObject(iterator.next());    
            }
            generator.writeEndArray();
        } else {
            generator.writeObject(x.getValue());
        }
    }
    
    private byte[] genRandomKey() {
        byte[] bytes = new byte[8];
        ByteBuffer buf = ByteBuffer.wrap(bytes);  
        buf.putLong(seq.incrementAndGet());
        return bytes;
    }

    public void setKeyName(String keyName) {
        this.keyName = keyName;
    }
}
