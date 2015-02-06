/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Ignore;

public class FuzzyMatcherTest {

  public static void main(String[] args) {
    String originalString = "<a>ok<b>5678</b><c>this is it</c></a>";
    String regex = "<b>\\d+</b>";
    System.out.println(regex);
    String replacement = "{{" + regex.replace("\\", "\\\\") + "}}";
    System.out.println(originalString.replaceAll(regex, replacement));
  }

  @Ignore
  // Not Required
  public void testExactMatchConsumer() throws PatternNotMatchException {
    FuzzyMatcher.Consumer exactMatchConsumer = new FuzzyMatcher.ExactMatchConsumer("abc");

    String remainingString = exactMatchConsumer.consume("abcdefg");

    assertEquals("defg", remainingString);

    try {
      exactMatchConsumer.consume("1234abcdef");
    }
    catch (PatternNotMatchException pe) {
      return;
    }

    fail("Should not reach here");
  }

  @Ignore
  // Not Required
  public void testFuzzyFileMatcher() {
    String expectationFilename = "test/junit/expectedResponse.txt";
    String matchedContentFilename = "test/junit/matchedResponse.txt";
    String unmatchedContentFilename = "test/junit/unmatchedResponse.txt";

    FuzzyMatcher fuzzyMatcher = new FuzzyMatcher(expectationFilename);

    assertTrue(fuzzyMatcher.matchFile(matchedContentFilename));
    assertFalse(fuzzyMatcher.matchFile(unmatchedContentFilename));
  }

  @Ignore
  // Not Required
  public void testFuzzyMatcher() {
    String expectedString = "abc{{\\d+}}xyz";
    FuzzyMatcher fuzzyMatcher = new FuzzyMatcher();
    fuzzyMatcher.setExpectation(expectedString);

    assertTrue(fuzzyMatcher.match("abc1234xyz"));
    assertFalse(fuzzyMatcher.match("ab1234xyz"));
  }

  @Ignore
  // Not Required
  public void testRegExMatchConsumer() throws PatternNotMatchException {
    FuzzyMatcher.Consumer regexMatchConsumer = new FuzzyMatcher.RegExMatchConsumer("\\d+");

    String remainingString = regexMatchConsumer.consume("1234efg");

    assertEquals("efg", remainingString);

    try {
      regexMatchConsumer.consume("abcdef");
    }
    catch (PatternNotMatchException pe) {
      return;
    }

    fail("Should not reach here");
  }

  @Ignore
  // Not Required
  public void testReplacement() {

    String originalString = "<a>ok<b>5678</b><c>this is it</c></a>";
    String[] regexArray = { "<b>\\d+</b>" };

    FuzzyMatcher fuzzyMatcher = new FuzzyMatcher();
    String finalString = fuzzyMatcher.replace(originalString, regexArray);

    System.out.println(finalString);

    assertEquals(finalString, "<a>ok{{<b>.*?</b>}}<c>this is it</c></a>");
  }

}
