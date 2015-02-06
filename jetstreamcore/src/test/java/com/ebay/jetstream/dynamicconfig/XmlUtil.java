/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.dynamicconfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;


public final class XmlUtil {

	public static String getBeanDefinition(String beanId, File file) throws Exception {
		String beanDefinition = null;
		InputStream stream = null;
		try{
			stream = new FileInputStream(file);
			Element rootElement = loadRootElement(stream);
			beanDefinition = loadBeanDef(rootElement, beanId);
			
		} catch(Exception e) {
			throw e;
		} finally{
			if(stream != null)
				stream.close();
		}
		
		return beanDefinition;
	}
	
	public static List<String> getBeanIds(File file) throws Exception {
		List<String> beanIds = new ArrayList<String>();
		InputStream stream = null;
		try{
			stream = new FileInputStream(file);
			Element rootElement = loadRootElement(stream);
			
			beanIds = findAllBeanIds(rootElement, "bean");
		} catch(Exception e) {
			throw e;
		}finally{
			if(stream != null)
				stream.close();
		}
		
		return beanIds;
	}
	
	private static String getStringFromDocument(Node doc)
	{
	    try
	    {
	       DOMSource domSource = new DOMSource(doc);
	       StringWriter writer = new StringWriter();
	       StreamResult result = new StreamResult(writer);
	       TransformerFactory tf = TransformerFactory.newInstance();
	       Transformer transformer = tf.newTransformer();
	       transformer.transform(domSource, result);
	       return writer.toString();
	    }
	    catch(TransformerException ex)
	    {
	       ex.printStackTrace();
	       return null;
	    }
	} 
	
	private static Element loadRootElement(InputStream inputStream) throws Exception {
        // Get XML document from the URL
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    
        //as a default, set validating to false
        factory.setValidating(false);
        factory.setNamespaceAware(true);
        DocumentBuilder builder = factory.newDocumentBuilder();
        InputSource inputSource = new InputSource(inputStream);
        Document doc = builder.parse(inputSource);
        return doc.getDocumentElement();
    }
	
	private static String loadBeanDef(Element rootElement, String beanId) {
		String beanDefinition = null;
		Element beanElement = findChildById(rootElement, "bean", beanId);
		
		if(beanElement != null) {
			beanDefinition = getStringFromDocument(beanElement);
		}
		
		return beanDefinition;
	}
	
	private static Element findChildById(Element parent, String tag, String attributeIdValue) {
		// First check to see if any parameters are null
		if (parent == null || tag == null)
			return null;
		// Check to see if this is the element we are interested in. This is
		// redundant apart from first call, but keep in to keep functionality
//		if (nodeNameEqualTo(parent, tag))
//			return parent;
		// Get all the children
		NodeList list = parent.getChildNodes();
		int listCount = list.getLength();
		for (int k = 0; k < listCount; k++) {
			Node child = list.item(k);
			// If the node is not an element, ignore it
			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;
			// Check to see if this node is the node we want
			if (nodeNameEqualTo(child, tag)) {
				if(((Element)child).getAttribute("id").equals(attributeIdValue)){
					return (Element) child;	
				}
			}
		}
		// Now that we have checked all children, we can recurse
		for (int k = 0; k < listCount; k++) {
			Node child = list.item(k);
			// If the node is not an element, ignore it
			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;
			Element result = findChildById((Element) child, tag, attributeIdValue);
			if (result != null)
				return result;
		}
		return null;
	}
	
	private static List<String> findAllBeanIds(Element parent, String tag) {
		List<String> beanIds = new ArrayList<String>();
		// First check to see if any parameters are null
		if (parent == null || tag == null)
			return null;
		// Check to see if this is the element we are interested in. This is
		// redundant apart from first call, but keep in to keep functionality
//		if (nodeNameEqualTo(parent, tag))
//			return parent;
		// Get all the children
		NodeList list = parent.getChildNodes();
		int listCount = list.getLength();
		for (int k = 0; k < listCount; k++) {
			Node child = list.item(k);
			// If the node is not an element, ignore it
			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;
			// Check to see if this node is the node we want
			if (nodeNameEqualTo(child, tag)) {
				beanIds.add(((Element)child).getAttribute("id"));
			}
		}
		
		return beanIds;
	}

	/**
	 * Check whether a name of given node is equal to a given name. Strips
	 * namespace (if any). Case-sensitive.
	 */
	private static boolean nodeNameEqualTo(Node node, String target) {
		if (node == null || target == null)
			return false;
		String name = node.getNodeName();
		// If target contains namespace, require exact match
		if (target.indexOf(':') < 0) {
			int index = name.indexOf(':');
			if (index >= 0)
				name = name.substring(index + 1); // Strip namespace
		}
		return name.equals(target);
	}
	
	
	
	public static void main(String[] args) throws IOException {
		File file = new File("C:" + File.separator + "TestBean2.xml");
		//String test = "default-lazy-init=\"false\">";
		InputStream stream = null;
		try {
			
			stream = new FileInputStream(file);
			Element rootElement = XmlUtil.loadRootElement(stream);
			String beanDef = XmlUtil.loadBeanDef(rootElement, "SystemPropertiesConfiguration");
			
			System.out.println(" bean definition is : " +beanDef);
		} catch(Exception e) {
			System.out.println(e.getMessage());
		}finally{
			if(stream != null)
				stream.close();
		}
		
	}
}