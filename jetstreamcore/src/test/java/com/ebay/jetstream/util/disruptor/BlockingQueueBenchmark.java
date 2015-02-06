/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.disruptor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
//import java.util.concurrent.LinkedTransferQueue;

public class BlockingQueueBenchmark {

    public static void main(String[] args) throws Exception {
        int bufferSize = 262144;
        int producerNum = 1;
        int batchSize = 0;
        char t = 'd';

        if (args.length == 0) {
            System.out.println("Usage: type=dbq|lbq|abq|tq batch=0 producer=1 bufferSize=262144");
        }
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("type=")) {
                t = args[i].substring("type=".length()).charAt(0);
            } else if (args[i].startsWith("batch=")) {
                batchSize = Integer.parseInt(args[i].substring("batch=".length()));
            } else if (args[i].startsWith("producer=")) {
                producerNum = Integer.parseInt(args[i].substring("producer=".length()));
            } else if (args[i].startsWith("bufferSize=")) {
                bufferSize = Integer.parseInt(args[i].substring("bufferSize=".length()));
            } else {
                System.out.println("Usage: type=dbq|lbq|abq|tq batch=256 producer=1 bufferSize=65536");
            }
        }

        System.out.println("Producers :" + producerNum + ", buffer size: " + bufferSize + ", batch:" + batchSize);
        switch (t) {
        case 'd':
            testQueue(new SingleConsumerDisruptorQueue<String>(bufferSize, false), producerNum, batchSize);
            break;
        case 'l':
            testQueue(new LinkedBlockingQueue<String>(bufferSize), producerNum, batchSize);
            break;
        case 'a':
            testQueue(new ArrayBlockingQueue<String>(bufferSize), producerNum, batchSize);
            break;
        case 't':
//            testQueue(new LinkedTransferQueue<String>(), producerNum, batchSize);
            break;
        }

    }

    private static void testQueue(final BlockingQueue<String> queue, int producerNum, final int batchSize)
            throws InterruptedException {
        final long count = 1 << 25; // 32M
        final long cc = count * producerNum;

        Runnable singleConsumer = new Runnable() {

            @Override
            public void run() {
                int c = 0;
                while (c < cc) {
                    try {
                        queue.take();
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                    c++;
                }
            }
        };

        Runnable batchConsumer = new Runnable() {

            @Override
            public void run() {
                int c = 0;
                List<String> s = new ArrayList<String>(batchSize);
                while (c < cc) {
                    try {
                        queue.take();
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                    c++;

                    int avail = queue.size();
                    if (avail > 0) {
                        s.clear();
                        c += queue.drainTo(s, Math.min(avail, batchSize));
                    }
                }
            }
        };

        List<Runnable> producers = new ArrayList<Runnable>();
        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < producerNum; i++) {
            Runnable producer = new Runnable() {

                @Override
                public void run() {
                    int c = 0;
                    while (c < count) {
                        try {
                            queue.put("x");
                        } catch (Throwable ex) {
                            ex.printStackTrace();
                        }
                        c++;
                    }
                }
            };
            producers.add(producer);
            threads.add(new Thread(producer));
        }

        Thread t2;
        if (batchSize > 1) {
            t2 = new Thread(batchConsumer);
        } else {
            t2 = new Thread(singleConsumer);
        }
        long begin = System.currentTimeMillis();
        t2.start();
        for (Thread t : threads) {
            t.start();
        }
        t2.join();
        long end = System.currentTimeMillis();
        for (Thread t : threads) {
            t.join();
        }
        if (queue.size() != 0) {
            System.out.println("Error" + queue.size());
        }
        System.out.println(queue.getClass().getSimpleName() + " transfer rate : " + (cc / (end - begin))
                + " per ms, Used " + (end - begin) + "ms for " + cc);
    }

}
