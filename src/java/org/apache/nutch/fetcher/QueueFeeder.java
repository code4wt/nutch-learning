/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.fetcher;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.nutch.crawl.CrawlDatum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

/**
 * This class feeds the queues with input items, and re-fills them as items
 * are consumed by FetcherThread-s.
 */
public class QueueFeeder extends Thread {

    private static final Logger LOG = LoggerFactory
            .getLogger(MethodHandles.lookup().lookupClass());

    private RecordReader<Text, CrawlDatum> reader;
    private FetchItemQueues queues;
    private int size;
    private long timelimit = -1;

    public QueueFeeder(RecordReader<Text, CrawlDatum> reader,
                       FetchItemQueues queues, int size) {
        this.reader = reader;
        this.queues = queues;
        this.size = size;
        this.setDaemon(true);
        this.setName("QueueFeeder");
    }

    public void setTimeLimit(long tl) {
        timelimit = tl;
    }

    public void run() {
        boolean hasMore = true;
        int cnt = 0;
        int timelimitcount = 0;
        while (hasMore) {
            /* 测试当前时间是否大于 timelimit，大于则读取所有 entry，但是不处理。
             * timelimit = System.currentTimeMillis() + timelimit * 60 * 1000
             * timelimit 用于控制 QueueFeeder 的运行时长，进而就能控制下载Job的运行时长，
             * 防止过长时间运行导致目标站点压力过大
             */
            if (System.currentTimeMillis() >= timelimit && timelimit != -1) {
                // enough .. lets' simply
                // read all the entries from the input without processing them
                try {
                    Text url = new Text();
                    CrawlDatum datum = new CrawlDatum();
                    hasMore = reader.next(url, datum);
                    timelimitcount++;
                } catch (IOException e) {
                    LOG.error("QueueFeeder error reading input, record " + cnt, e);
                    return;
                }
                continue;
            }
            int feed = size - queues.getTotalSize();
            if (feed <= 0) {
                // queues are full - spin-wait until they have some free space
                // feed <= 0，意味着 FetchItemQueue 无空闲空间，这里进行自旋等待，知道有空闲空间为止
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                }

                continue;
            } else {
                LOG.debug("-feeding " + feed + " input urls ...");
                while (feed > 0 && hasMore) {
                    try {
                        Text url = new Text();
                        CrawlDatum datum = new CrawlDatum();
                        hasMore = reader.next(url, datum);
                        if (hasMore) {
                            queues.addFetchItem(url, datum);
                            cnt++;
                            feed--;
                        }
                    } catch (IOException e) {
                        LOG.error("QueueFeeder error reading input, record " + cnt, e);
                        return;
                    }
                }
            }
        }
        LOG.info("QueueFeeder finished: total " + cnt
                + " records + hit by time limit :" + timelimitcount);
    }
}
