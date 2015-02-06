/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka.support;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.channel.kafka.KafkaMessageSerializer;
import com.google.common.base.Charsets;

/**
 * An avro serializer to convert jetstream event (flat map) to avro record.
 * 
 * @author xingwang
 *
 */
public class AvroMessageSerializer implements KafkaMessageSerializer {
    private String keyName = com.ebay.jetstream.event.JetstreamReservedKeys.MessageAffinityKey.toString();
    private final JsonFactory factory = new JsonFactory();
    private final AtomicLong seq = new AtomicLong(0);
    private static final Schema schema;
    private GenericDatumWriter<Record> writer;
    private GenericDatumReader<Record> reader;
    private boolean filterOutNullValue = false;

    private static final String MAP_FIELD_NAME = "attr";
    private static ThreadLocal<BinaryDecoder> decoderHolder = new ThreadLocal<BinaryDecoder>();
    private static ThreadLocal<BinaryEncoder> encoderHolder = new ThreadLocal<BinaryEncoder>();
    private static ThreadLocal<Record> recordHolder = new ThreadLocal<Record>() {

        @Override
        protected Record initialValue() {
            return new GenericData.Record(schema);
        }
        
    };
    
    static {
        String schemaDefinition = "{\"namespace\": \"jetstream\", \"name\": \"event\",\"type\": \"record\","
                + "\"fields\": [ {\"name\": \""
                + MAP_FIELD_NAME
                + "\", \"type\": {\"type\": \"map\", \"values\" : "
                + "[\"null\",\"boolean\",\"int\",\"long\",\"float\",\"double\",\"bytes\",\"string\", "
                + "{\"type\": \"array\", \"items\": [\"boolean\",\"int\",\"long\",\"float\",\"double\",\"string\"]}]}}]}";

        schema = new Parser().parse(schemaDefinition);
        GenericData.setStringType(schema, GenericData.StringType.String);
        GenericData.setStringType(schema.getField(MAP_FIELD_NAME).schema(), GenericData.StringType.String);
    }
    public AvroMessageSerializer() {
        writer = new GenericDatumWriter<Record>(schema);
        reader = new GenericDatumReader<Record>(schema) {
            @SuppressWarnings("rawtypes")
            @Override
            protected Class findStringClass(Schema schema) {
                return String.class;
            }
            
        };
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

    public void setFilterOutNullValue(boolean b) {
        filterOutNullValue = b;
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
    
    public String jsonMap(Map<String, Object> event) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JsonGenerator generator;
        try {
            generator = factory.createJsonGenerator(out, JsonEncoding.UTF8);
        } catch (IOException e) {
            throw new IllegalStateException("Can not create json generator", e);
        }

        try {
            encodeMap(event, generator);
            generator.close();
        } catch (JsonGenerationException e) {
            throw new IllegalArgumentException(
                    "Fail to convert the object to json format", e);
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Fail to convert the object to json format", e);
        }
        return new String(out.toByteArray());
    }
    
    @SuppressWarnings("unchecked")
    private void encodeMap(Map<String, Object> event, JsonGenerator generator)
            throws IOException, JsonGenerationException,
            JsonProcessingException {
        generator.writeStartObject();
        for (Map.Entry<String, Object> x : event.entrySet()) {
            generator.writeFieldName(x.getKey());
            if (x.getValue() == null) {
                //We assume all value type should be primitive.
                generator.writeObject(x.getValue());
            } else if (Map.class.isAssignableFrom(x.getValue().getClass())) {
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
        generator.writeEndObject();
    }

    
    @SuppressWarnings("unchecked")
    private ArrayList toArrayList(Object array) {
        ArrayList r = new ArrayList(); 
        int length = Array.getLength(array);
        for (int i = 0; i < length; i++) {
            r.add(Array.get(array, i));    
        }
        return r;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public byte[] encodeMessage(JetstreamEvent event) {
        Record record = recordHolder.get();
        //Should do convertion before serialization.
        Map<String, Object> m = new HashMap<String, Object>(event.size());
        for (Map.Entry<String, Object> e : event.entrySet()) {
            if (e.getValue() == null) {
                if (!filterOutNullValue) {
                    m.put(e.getKey(), null);
                }
                continue;
            } 
            Class<? extends Object> clazz = e.getValue().getClass();
            if (clazz == int.class || clazz == Integer.class
                    || clazz == float.class || clazz == Float.class
                    || clazz == boolean.class || clazz == Boolean.class
                    || clazz == long.class || clazz == Long.class
                    || clazz == double.class || clazz == Double.class
                    || clazz == String.class) {
                m.put(e.getKey(), e.getValue());    
            } else if (clazz == byte.class || clazz == Byte.class
                    || clazz == short.class || clazz == Short.class ) {
                m.put(e.getKey(), ((Number) e.getValue()).intValue());
            } else if (clazz == char.class || clazz == Character.class) {
                m.put(e.getKey(), ((int) ((Character) e.getValue()).charValue()));
            } else if (Map.class.isAssignableFrom(e.getValue().getClass())){
                m.put(e.getKey(), jsonMap((Map<String, Object>)e.getValue()));
            } else if (e.getValue().getClass().isArray()) {
                m.put(e.getKey(), toArrayList(e.getValue()));
            } else if (e.getValue() instanceof Collection) {
                m.put(e.getKey(), e.getValue());
            } else {
                throw new IllegalArgumentException("Unsupported value type : " + e.getValue().getClass());
            }
        }
        
        record.put(MAP_FIELD_NAME, m);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder reusedEncoder = encoderHolder.get();
        BinaryEncoder directBinaryEncoder = EncoderFactory.get().directBinaryEncoder(out, reusedEncoder);
        if (reusedEncoder == null) {
            encoderHolder.set(directBinaryEncoder);
        }
        try {
            writer.write(record, directBinaryEncoder);
        } catch (IOException e) {
            throw new IllegalArgumentException("Can not encode the event to avro format: " + event, e);
        }
        return out.toByteArray();
    }

    @SuppressWarnings("unchecked")
    @Override
    public JetstreamEvent decode(byte[] key, byte[] message) {
        ByteArrayInputStream stream = new ByteArrayInputStream(message);
        BinaryDecoder reusedDecoder = decoderHolder.get();
        BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(stream, reusedDecoder);
        if (reusedDecoder == null) {
            decoderHolder.set(decoder);
        }
        Record object;
        try {
            object = reader.read(null, decoder);
            Map<String, Object> m = (Map<String, Object>) object.get(MAP_FIELD_NAME);
            return new JetstreamEvent(m);
        } catch (IOException e) {
            throw new IllegalArgumentException("Can not read the avro message", e);
        }
    }

}
