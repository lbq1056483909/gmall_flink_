package com.atguigu.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @ClassName MyKafkaUtil
 * @Description TODO
 * @Author 10564
 * @Date 2022/1/24 13:11
 * @Version 1.0
 */
public class MyKafkaUtil {
    private static String DEFAULT_TOPIC = "dwd_default_topic";
    private static String KAFKA_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    private static Properties properties = new Properties();

    static {
        properties.setProperty("bootstrap.servers", KAFKA_SERVER);
    }

    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        //向kafka里面发数据,kafka生产者
        return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), properties);

    }

    //因为kafka流入dwd层的事实表太多,主题不能确定,所以,当有多个主题时就用这个
    public static <T> FlinkKafkaProducer<T> getKafkaSink(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC,
                kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    //从kafka里面读取数据,kafka消费者,参数里填上topic和消费者组
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //返回值是一个kafka消费者,填到主方法的env.addSource中去.
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }
    //拼接Kafka相关属性到DDL
    public static String getKafkaDDL(String topic,String groupId){
        String ddl="'connector' = 'kafka', " +
                " 'topic' = '"+topic+"',"   +
                " 'properties.bootstrap.servers' = '"+ KAFKA_SERVER +"', " +
                " 'properties.group.id' = '"+groupId+ "', " +
                "  'format' = 'json', " +
                "  'scan.startup.mode' = 'latest-offset'  )";
        return  ddl;
    }



    //以下为测试代码,测试这个特殊的生产者当作普通生产者使用穿入一个主题,一个String
//    public static void main(String[] args) throws Exception {
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9990);
//        FlinkKafkaProducer<String> kafkatest = getKafkaSink(new KafkaSerializationSchema<String>() {
//            @Override
//            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
//                return new ProducerRecord<byte[], byte[]>("kafkatest", "hell0".getBytes());
//
//            }
//        });
//        dataStreamSource.addSink(kafkatest);
//        env.execute();
//    }
}

