package me.ubmagh.KMeansMapReduce_1dpoints;

import org.apache.hadoop.conf.Configuration;

public class ConnectionToHDFS {
    private static Configuration conf;
    private static final String url = "hdfs://localhost:9000/";
    static {
        System.setProperty("HADOOP_USER_NAME", "root");
        conf = new Configuration();
        conf.set("fs.defautlFS", "hdfs://localhost:9000/");
        conf.set("replication", "1");
    }

    public static Configuration getConf() {
        return conf;
    }

    public static String getUrl() {
        return url;
    }
}

