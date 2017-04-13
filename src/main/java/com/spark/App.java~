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

public class App {

    private static final Pattern SPACE = Pattern.compile(" ");

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

        //********************************************************************
        // Load and parse data to Kmeans Model
//        String path = "data/mllib/kmeans_data.txt";
//        JavaRDD<String> dataset = sc.textFile(path);
//        JavaRDD<Vector> parsedData = dataset.map(s -> {
//            String[] sarray = s.split(" ");
//            double[] values = new double[sarray.length];
//            for (int i = 0; i < sarray.length; i++) {
//                values[i] = Double.parseDouble(sarray[i]);
//            }
//            return Vectors.dense(values);
//        });
        
      List<Vector> points = Arrays.asList(
      Vectors.dense(1.0, 2.0, 6.0),
      Vectors.dense(1.0, 3.0, 0.0),
      Vectors.dense(1.0, 4.0, 6.0)
    );
     JavaRDD<Vector> train = sc.parallelize(points, 2);
        
        int numClusters = 2;
        int numIterations = 20;
        KMeansModel clusters = KMeans.train(train.rdd(), numClusters, numIterations);
        //************************************************************************
        // connect to kafka
        topicMap.put(topic, new Integer(3));
        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, host + ":2181", "test-group", topicMap);

        JavaDStream<String> data = messages.map(new Function<Tuple2<String, String>, String>() {
            public String call(Tuple2<String, String> message) {
                String msg = message._2();
                String rst="-";
                try {
                    String[] events = msg.split(",");
                    double[] examples = new double[events.length];
                    for (int i = 0; i < events.length; i++) {
                        examples[i] = Double.parseDouble(events[i]);
                    }
                  rst=msg+"======================================>"+clusters.predict(Vectors.dense(examples));
                 // rst=""+clusters.predict(Vectors.dense(1.0, 3.0, 0.0));
                    

                } catch (Exception e) {

                }
                return rst ;

            }
        }
        );

        data.print();
        jssc.start();
        jssc.awaitTermination();

    }

}

