/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.scattergather;

import java.util.Iterator;
import java.util.List;

import org.junit.Test;

public class ScatterGatherTest {

  
	@Test
	public void testScatterGather(){

    TaskMasterFactory tmf = new TaskMasterFactory();
    TaskMaster tm1 = tmf.create();

    for (int i = 0; i < 10; i++) {
      DemoTask task = new DemoTask(i, 120);
      tm1.submitTask(task);
    }

    List<Task> executedTasks = tm1.executeTasks(100);

    Iterator<Task> itr = executedTasks.iterator();

    while (itr.hasNext()) {

      System.out.println("TM1 task executed = " + itr.next().isExecuted()); //KEEPME

    }

    TaskMaster tm2 = tmf.create();
    for (int i = 10; i < 20; i++) {
      DemoTask task = new DemoTask(i, 10);
      tm2.submitTask(task);
    }

    List<Task> executedTasks1 = tm2.executeTasks(100);

    itr = executedTasks1.iterator();

    while (itr.hasNext()) {

      System.out.println("TM2 task executed = " + itr.next().isExecuted()); //KEEPME

    }

    System.out.println(".......Test Ended ......."); //KEEPME
  }

}
