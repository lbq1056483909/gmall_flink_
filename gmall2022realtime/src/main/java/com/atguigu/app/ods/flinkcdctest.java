package com.atguigu.app.ods;


import com.atguigu.app.func.MyCustomDeserializer;
import com.atguigu.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class flinkcdctest {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
//        //2.1 开启Checkpoint,每隔5秒钟做一次CK
//        env.enableCheckpointing(5000L);
//        //2.2 指定CK的一致性语义
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        //2.3 设置任务关闭的时候保留最后一次CK数据
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //2.4 指定从CK自动重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
//        //2.5 设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flinkCDC"));
//        //2.6 设置访问HDFS的用户名
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        DebeziumSourceFunction<String> sourceFunction = MySqlSource
                .<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall2021_flink_realtime")
                .tableList("gmall2021_flink_realtime.table_process")
                .username("root")
                .password("123456")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyCustomDeserializer())
                .build();

        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        //将数据发送至Kafka的ods_base_db主题中,这个就是将mysql中的数据通过flinkcdc实时的传送到kafka中
        //并且这个数据已经经过自定义的反序列化器进行处理

        streamSource.print();

        env.execute();


    }
}
