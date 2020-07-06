package com.ifchange.flink.sink;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class MyKafkaSink extends RichSinkFunction<Tuple2<String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(MyKafkaSink.class);

    private KafkaProducer<String, String> producer;

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info("call my kafka sink open....");
        Properties props2 = new Properties();
        props2.setProperty("bootstrap.servers", "192.168.9.129:9092,192.168.9.130:9092,192.168.9.131:9092,192.168.9.132:9092,192.168.9.133:9092");
        props2.put("acks", "all");// 记录完整提交，最慢的但是最大可能的持久化
        props2.put("retries", 3);// 请求失败重试的次数
        props2.put("batch.size", 5);// batch的大小
        props2.put("linger.ms", 1);// 默认情况即使缓冲区有剩余的空间，也会立即发送请求，设置一段时间用来等待从而将缓冲区填的更多，单位为毫秒，producer发送数据会延迟1ms，可以减少发送到kafka服务器的请求数据
        props2.put("buffer.memory", 33554432);// 提供给生产者缓冲内存总量
        props2.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");// 序列化的方式，
        props2.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props2);
    }

    @Override
    public void invoke(Tuple2<String, String> tuple2, Context context) {
        String data = tuple2.f0;
        String topic = tuple2.f1;
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, data);
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (null == exception) {
                    int partition = metadata.partition();
                    long offset = metadata.offset();
                    String topic1 = metadata.topic();
                    LOG.info("data send to kafka success,topic:{}->partition:{}->offset:{}", topic1, partition, offset);
                }
                if (null == metadata) {
                    LOG.error("data send to kafka error:{}", exception.getMessage());
                }
            }
        });
    }

    @Override
    public void close() throws Exception {
        producer.close();
    }
}
