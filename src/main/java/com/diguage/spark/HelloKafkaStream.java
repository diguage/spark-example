package com.diguage.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class HelloKafkaStream {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("KafkaWordCount");
        JavaStreamingContext context = new JavaStreamingContext(conf, Durations.seconds(1));

        // JavaReceiverInputDStream<String> lines = context.socketTextStream("localhost", 9999);


        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put("dest", 1);
        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(context, "localhost:2181", "spark-stream", topicMap);
        JavaDStream<String> lines = messages.map(Tuple2::_2);


        JavaDStream<String> words = lines.flatMap(l -> Arrays.asList(l.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairs = words.mapToPair(w -> new Tuple2<>(w, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);

        wordCounts.print();

        context.start();

        context.awaitTermination();
    }
}
