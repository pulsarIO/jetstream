/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.application.dataflows;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * The simple implementation of directed graph.
 * 
 * @author weijin
 * 
 * @param <T>
 */
public class DirectedGraph<T> {

	private final Map<T, Set<T>> map = new HashMap<T, Set<T>>();

	public DirectedGraph(T[] ts) {
		for (T t : ts) {
			addNode(t);
		}
	}

	public DirectedGraph() {
	}

	public boolean addNode(T node) {
		if (map.containsKey(node)) {
			return false;
		}

		map.put(node, new HashSet<T>());

		return true;
	}

	public void addEdge(T start, T dest) {
		if ((!map.containsKey(dest) || !map.containsKey(start))) {
			throw new NoSuchElementException("Both nodes must be in the graph.");
		}

		map.get(start).add(dest);
	}

	public void removeEdge(T start, T dest) {
		if ((!map.containsKey(dest) || !map.containsKey(start))) {
			throw new NoSuchElementException("Both nodes must be in the graph.");
		}

		map.get(start).remove(dest);
	}

	public Set<T> edgesFrom(T node) {
		return map.get(node);
	}

	public Map<T, Set<T>> getMap() {
		return map;
	}

	public boolean isEmpty() {
		return map.isEmpty();
	}

	public Set<T> getNodes() {
		return map.keySet();
	}

	public String toString() {
		return map.toString();
	}

	public static void main(String[] args) {
		String[] strs = new String[] { "input1", "input2", "esperprocessor",
				"output1", "output2" };
		DirectedGraph<String> graph = new DirectedGraph<String>(strs);
		graph.addEdge("input1", "esperprocessor");
		graph.addEdge("input2", "esperprocessor");

		graph.addEdge("esperprocessor", "output1");
		graph.addEdge("esperprocessor", "output2");

		graph.addEdge("output2", "input1");

	}
}