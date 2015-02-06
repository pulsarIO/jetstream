/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 
 * 
 *
 */
public class FuzzyMatcher {
		
	public static interface Consumer {
		/**
		 * Take the original string, and check whether it begins with the matcher
		 * @param originalString
		 * @return the remaining string after match
		 */
		String consume(String originalString) throws PatternNotMatchException;
	}
	
	public static class ExactMatchConsumer implements Consumer {
		
		private String matcherString;
		
		public ExactMatchConsumer(String matcherString) {
			this.matcherString = matcherString;
		}
		
		public String consume(String originalString) throws PatternNotMatchException {
			if (originalString.startsWith(matcherString)) {
				return originalString.substring(matcherString.length());
			} else {
				throw new PatternNotMatchException();
			}
		}
		
		public String toString() {
			return "Exact:" + matcherString;
		}
	}
	
	public static class RegExMatchConsumer implements Consumer {
		
		private String matcherString;
		
		public RegExMatchConsumer(String matcherString) {
			this.matcherString = matcherString;
		}

		public String consume(String originalString) throws PatternNotMatchException {
			
			Pattern pattern = Pattern.compile(matcherString);
			Matcher matcher = pattern.matcher(originalString);
			if (matcher.find()) {
				if (matcher.start() == 0) {
					return originalString.substring(matcher.end());
				}
			}
			
			throw new PatternNotMatchException();
		}
		
		public String toString() {
			return "Regex:" + matcherString;
		}
	}
	
	private final static String regexStartPattern = "\\{\\{";
	private final static String regexEndPattern = "\\}\\}";
	
	private List<Consumer> listOfConsumers = new ArrayList<Consumer>();
	
	public FuzzyMatcher() {
	}
	
	public FuzzyMatcher(String fileContainingExpectedString) {
		setExpectation(FileUtils.readFileOrNull(fileContainingExpectedString));
	}

	public void setExpectation(String expectedString) {
		Pattern startPattern = Pattern.compile(regexStartPattern);
		Pattern endPattern = Pattern.compile(regexEndPattern);
				
		String remainingString = expectedString;
		
		while (true) {
			Matcher matcher1 = startPattern.matcher(remainingString);
			if (matcher1.find()) {
				listOfConsumers.add(new ExactMatchConsumer(remainingString.substring(0, matcher1.start())));

				String cutHead = remainingString.substring(matcher1.end());
				Matcher matcher2 = endPattern.matcher(cutHead);
				if (matcher2.find()) {
					String regex =
						cutHead.substring(0, matcher2.start());
					
					listOfConsumers.add(new RegExMatchConsumer(regex));
					
					remainingString = cutHead.substring(matcher2.end());
					continue;
				}
			}
			
			listOfConsumers.add(new ExactMatchConsumer(remainingString));			
			break;
		}
	}
		
	public boolean match(String originalString) {
		String remainingString = originalString;
		
		Iterator<Consumer> it = listOfConsumers.iterator();
		
		while (it.hasNext() && remainingString.length() > 0) {
			Consumer consumer = it.next();
			try {
				remainingString = consumer.consume(remainingString);
			} catch (PatternNotMatchException e) {
				return false;
			}
		}
		
		return true;
	}
	
	public boolean matchFile(String filename) {
		String content = FileUtils.readFileOrNull(filename);
		return match(content);
	}
	
	public String replace(String originalString, String[] regexArray) {
		
		String resultString = originalString;
		for (String regex : regexArray) {
			String replacedForm = "{{" + regex.replace("\\", "\\\\") + "}}";
			resultString = resultString.replaceAll(regex, replacedForm);		
		}
		
		return resultString;
	}
	
}