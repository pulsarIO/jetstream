/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka.support;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamConstants;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.channel.kafka.KafkaMessageSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Charsets;

/**
 * An implementation to use java serialization to store whole jetstream
 * event to kafka and replay it in the future.
 * 
 * @author xingwang
 *
 */
public class AdvicedEventKafkaSerializer implements KafkaMessageSerializer {
    private static class KryoContext {
        private Kryo kryo = new Kryo();
        private Output out;
        
        public KryoContext() {
            out = new Output(4096);
        }
        public Kryo getKryo() {
            return kryo;
        }
        public Output getOut() {
            return out;
        }
        
    } 
    private static final byte KRYO_STREAM_VERSION = (byte) 0x00;
	private static final byte JAVA_STREAM_VERSION = (byte) ObjectStreamConstants.STREAM_VERSION; // Java is 0x5
	private String keyName = com.ebay.jetstream.event.JetstreamReservedKeys.MessageAffinityKey.toString();
	
	private boolean usreKryo = true;

    private AtomicLong seq = new AtomicLong(0);

    private static ThreadLocal<KryoContext> kryoContextHolder = new ThreadLocal<KryoContext>() {
        @Override
        protected KryoContext initialValue() {
            return new KryoContext();
        }
    };
	
    @SuppressWarnings("cast")
    @Override
	public JetstreamEvent decode(byte[] key, byte[] message) {
        if (message[0] == KRYO_STREAM_VERSION) {
            return kryoDeserialize(message);
        }
		return (JetstreamEvent) deserialize(message);
	}
    
    private JetstreamEvent deserialize(byte[] data) {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ObjectInputStream ios = null;
        try {
            ios = new ObjectInputStream(in);
            return (JetstreamEvent) ios.readObject();
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Fail to deserialize the object", e);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(
                    "Fail to deserialize the object", e);
        } finally {
            if (ios != null) {
                try {
                    ios.close();
                } catch (IOException e) {
                }
            }
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

	@Override
	public byte[] encodeMessage(JetstreamEvent event) {
	    if (usreKryo) {
	        return kryoSerialize(event);
	    }
		return serialize(event);
	}

    private byte[] genRandomKey() {
        byte[] bytes = new byte[8];
        ByteBuffer buf = ByteBuffer.wrap(bytes);  
        buf.putLong(seq.incrementAndGet());
        return bytes;
    }

	private JetstreamEvent kryoDeserialize(byte[] data) {
        Kryo kryo = kryoContextHolder.get().getKryo();
        Input input = new Input(new ByteArrayInputStream(data));
        input.readByte(); // skip first byte
        return (JetstreamEvent) kryo.readClassAndObject(input);
    }
	
    private byte[] kryoSerialize(JetstreamEvent o) {
        KryoContext kryoContext = kryoContextHolder.get();
        Kryo kryo = kryoContext.getKryo();
        Output output = kryoContext.getOut();
        output.clear();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        output.setOutputStream(out);
        output.writeByte(KRYO_STREAM_VERSION);
        kryo.writeClassAndObject(output, o);
        output.flush();
        try {
            out.flush();
            out.close();
        } catch (IOException e) {
            //ingore
        }
        
        output.close();
        
        return out.toByteArray();
    }

    private byte[] serialize(JetstreamEvent o) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(out);
            oos.writeObject(o);

        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Fail to serializer the object of class + " + o.getClass(),
                    e);
        } finally {
            if (oos != null) {
                try {
                    oos.close();
                } catch (IOException e) {
                }
            }
        }

        return out.toByteArray();
    }
    
	
    public void setKeyName(String keyName) {
		this.keyName = keyName;
	}

    public void setUseKryo(boolean useKryo) {
        this.usreKryo = useKryo;
    }

}
