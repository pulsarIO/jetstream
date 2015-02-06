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
package com.ebay.jetstream.configurationmanagement;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import com.ebay.jetstream.config.mongo.JetStreamBeanConfigurationDo;

/**
 * The utility class that splits and parses the spring beans.
 * 
 * @author weijin
 * 
 */
public class ConfigurationManagementXMLParser {
    private static String XML_HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
    private static String NAME_SPACE = "xmlns=\"http://www.springframework.org/schema/beans\"";

    private static final ConfigurationManagementXMLParser instance = new ConfigurationManagementXMLParser();

    static {
        System.setProperty("javax.xml.transform.TransformerFactory",
                "com.sun.org.apache.xalan.internal.xsltc.trax.TransformerFactoryImpl");
    }

    public static ConfigurationManagementXMLParser getInstance() {
        return instance;
    }

    public static String getId(XMLStreamReader reader) {
        for (int i = 0; i < reader.getAttributeCount(); ++i) {

            if (reader.getAttributeLocalName(i).equals("id")) {
                return reader.getAttributeValue(i);
            }
        }

        return null;
    }

    public static String getId(String beanDefinition) throws FactoryConfigurationError, XMLStreamException {
        XMLStreamReader reader = XMLInputFactory.newInstance()
                .createXMLStreamReader(new StringReader(beanDefinition));
        reader.nextTag();
        if (reader.nextTag() == XMLStreamConstants.START_ELEMENT) {
            return getId(reader);
        }
        return null;
    }

    public static String getSource(String source) {
        int pos = source.indexOf(XML_HEADER);
        if (pos != -1) {
            source.substring(pos + XML_HEADER.length());
        }
        pos = source.indexOf(NAME_SPACE);
        if (pos != -1) {
            source = "<bean" + source.substring(pos + NAME_SPACE.length());
        }

        return source;
    }

    public List<JetStreamBeanConfigurationDo> parse(String source)
            throws FileNotFoundException, XMLStreamException,
            TransformerException {
        List<JetStreamBeanConfigurationDo> list = new ArrayList<JetStreamBeanConfigurationDo>();

        XMLInputFactory xmlInputfactory = XMLInputFactory.newInstance();
        TransformerFactory transformerFactory = TransformerFactory
                .newInstance();
        XMLStreamReader reader = xmlInputfactory
                .createXMLStreamReader(new StringReader(source));
        reader.nextTag();
        Transformer t = transformerFactory.newTransformer();
        t.setOutputProperty(OutputKeys.INDENT, "yes");
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        while (reader.nextTag() == XMLStreamConstants.START_ELEMENT) {
            JetStreamBeanConfigurationDo doObj = new JetStreamBeanConfigurationDo();
            doObj.setBeanName(getId(reader));
            t.transform(new StAXSource(reader), new StreamResult(bao));
            doObj.setBeanDefinition(getSource(bao.toString()));
            list.add(doObj);
            bao.reset();
        }

        return list;
    }

    public String prettyFormat(String input) {
        Writer writer = null;
        try {
            Source xmlInput = new StreamSource(new StringReader(input));
            StringWriter stringWriter = new StringWriter();
            StreamResult xmlOutput = new StreamResult(stringWriter);
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            transformerFactory.setAttribute("indent-number", 4);
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.transform(xmlInput, xmlOutput);
            writer = xmlOutput.getWriter();
            return writer.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }finally{
            if(writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    //ignore
                }
            }
        }
    }
}
