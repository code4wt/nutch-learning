package org.apache.nutch.example;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.nutch.crawl.CrawlDatum;

import java.io.IOException;

/**
 * Created by code4wt on 17/6/3.
 */
public class SequenceFileReader {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        String path = "/Users/imtian/Temp/nutch/crawl/crawldb/current/part-00000/data";
        path = "/Users/imtian/Temp/nutch/crawl/segments/20170603003147/content/part-00000/data";
        path = "/Users/imtian/Temp/nutch/crawl/segments/20170603003147/parse_text/part-00000/data";
        path = "/Users/imtian/Temp/nutch/crawl/segments/20170603003147/parse_data/part-00000/data";
        Path dataPath = new Path(path);
        FileSystem fs=dataPath.getFileSystem(conf);
        SequenceFile.Reader reader=new SequenceFile.Reader(fs,dataPath,conf);
        Text key = new Text();
        CrawlDatum value=new CrawlDatum();
        while(reader.next(key,value)){
            System.out.println("key:"+key);
            System.out.println("value:"+value);
        }
        reader.close();
    }
}
