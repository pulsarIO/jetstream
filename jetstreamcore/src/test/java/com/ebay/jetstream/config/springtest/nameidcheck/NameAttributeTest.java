/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config.springtest.nameidcheck;

import org.junit.Test;
import org.springframework.context.support.AbstractApplicationContext;

import com.ebay.jetstream.config.ApplicationInformation;
import com.ebay.jetstream.config.Configuration;

public class NameAttributeTest {

  @Test
  public void testNameAttribute(){
    AbstractApplicationContext container = new Configuration(new ApplicationInformation("JunitTesting-UnderstandingSpring", "0.0.0.0"), new String[] {"classpath:com/ebay/jetstream/config/springtest/nameidcheck/SampleSpringFile.xml"});

    OuterBean o1 = (OuterBean)container.getBean("IdPresent");

   
    System.out.println(o1.getBeanName());
    System.out.println(o1.getInnerBean().getBeanName());
  }
}
