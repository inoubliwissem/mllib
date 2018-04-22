/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.spark;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

/**
 *
 * @author inoubli
 */
public class FraudDetection {
     public static void main(String[] args) {
        // WSClient rq1=new WSClient();        JavaSparkContext sc = new JavaSparkContext();
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(1));
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        if (args.length != 2) {
            System.err.println("no arguments");
            System.exit(0);
        }
        String host = args[0];
        String topic = args[1];
        final String index = "";// args[2];
        final String doc = "";// args[3];
        
         String path = "data/mllib/kmeans_data.txt";
        JavaRDD<String> dataset = sc.textFile(path);
        JavaRDD<Vector> parsedData = dataset.map(s -> {
            String[] sarray = s.split(" ");
            double[] values = new double[sarray.length];
            for (int i = 0; i < sarray.length; i++) {
                values[i] = Double.parseDouble(sarray[i]);
            }
            return Vectors.dense(values);
        });

}
