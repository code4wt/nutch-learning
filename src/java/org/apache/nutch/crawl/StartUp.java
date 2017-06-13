package org.apache.nutch.crawl;

/**
 * Created by code4wt on 17/6/3.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.nutch.parse.ParseSegment;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class StartUp extends Configured implements Tool {
    public static final Logger LOG = LoggerFactory.getLogger(StartUp.class);

    private static String getDate() {
        return new SimpleDateFormat("yyyyMMddHHmmss").format
                (new Date(System.currentTimeMillis()));
    }


    /* Perform complete crawling and indexing (to Solr) given a set of root urls and the -solr
       parameter respectively. More information and Usage parameters can be found below. */
    public static void main(String args[]) throws Exception {
        Configuration conf = NutchConfiguration.create();
        conf.set("urlnormalizer.slashes.file", "/Users/imtian/github_repository/apache-nutch-1.13.ant/runtime/local/conf/slashes.txt");
        int res = ToolRunner.run(conf, new StartUp(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        /*种子所在文件夹*/
        Path rootUrlDir = new Path("/Users/imtian/Temp/nutch/urls");
        /*存储爬取信息的文件夹*/
        Path dir = new Path("/tmp","crawl-" + getDate());
        int threads = 50;
        /*广度遍历时爬取的深度，即广度遍历树的层数*/
        int depth = 2;
        long topN = 10;

        JobConf job = new NutchJob(getConf());
        FileSystem fs = FileSystem.get(job);

        if (LOG.isInfoEnabled()) {
            LOG.info("crawl started in: " + dir);
            LOG.info("rootUrlDir = " + rootUrlDir);
            LOG.info("threads = " + threads);
            LOG.info("depth = " + depth);
            if (topN != Long.MAX_VALUE)
                LOG.info("topN = " + topN);
        }

        Path crawlDb = new Path(dir + "/crawldb");
        Path linkDb = new Path(dir + "/linkdb");
        Path segments = new Path(dir + "/segments");
        Path indexes = new Path(dir + "/indexes");
        Path index = new Path(dir + "/index");

        Path tmpDir = job.getLocalPath("crawl" + Path.SEPARATOR + getDate());
        Injector injector = new Injector(getConf());
        Generator generator = new Generator(getConf());
        Fetcher fetcher = new Fetcher(getConf());
        ParseSegment parseSegment = new ParseSegment(getConf());
        CrawlDb crawlDbTool = new CrawlDb(getConf());
        LinkDb linkDbTool = new LinkDb(getConf());

        // initialize crawlDb
        injector.inject(crawlDb, rootUrlDir);
        int i;
        for (i = 0; i < depth; i++) {             // generate new segment
            Path[] segs = generator.generate(crawlDb, segments, -1, topN, System.currentTimeMillis());
            if (segs == null) {
                LOG.info("Stopping at depth=" + i + " - no more URLs to fetch.");
                break;
            }
            fetcher.fetch(segs[0], threads);  // fetch it
            if (!Fetcher.isParsing(job)) {
                parseSegment.parse(segs[0]);    // parse it, if needed
            }
            crawlDbTool.update(crawlDb, segs, true, true); // update crawldb
        }
        /*
        if (i > 0) {
            linkDbTool.invert(linkDb, segments, true, true, false); // invert links

            if (solrUrl != null) {
                // index, dedup & merge
                FileStatus[] fstats = fs.listStatus(segments, HadoopFSUtil.getPassDirectoriesFilter(fs));

                IndexingJob indexer = new IndexingJob(getConf());
                indexer.index(crawlDb, linkDb,
                        Arrays.asList(HadoopFSUtil.getPaths(fstats)));

                SolrDeleteDuplicates dedup = new SolrDeleteDuplicates();
                dedup.setConf(getConf());
                dedup.dedup(solrUrl);
            }

        } else {
            LOG.warn("No URLs to fetch - check your seed list and URL filters.");
        }
        */
        if (LOG.isInfoEnabled()) {
            LOG.info("crawl finished: " + dir);
        }
        return 0;
    }
}
