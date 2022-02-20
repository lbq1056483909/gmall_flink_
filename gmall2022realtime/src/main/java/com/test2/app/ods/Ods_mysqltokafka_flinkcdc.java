package com.test2.app.ods;

import com.test2.app.func.MyCustomDeserializer;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName Ods_mysqltokafka_flinkcdc
 * @Description TODO
 * @Author 10564
 * @Date 2022/2/10 16:17
 * @Version 1.0
 */
public class Ods_mysqltokafka_flinkcdc {
    public static void main(String[] args) throws Exception {
        //1.获取流的数据
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        //2.flink-cdc将读取binlog的位置信息以状态的方式保存在ck,
//        //如果想要做到断电续传,需要从Checkpoint或者savepoint启动程序
//        //2.1.开启Checkpoint,每间隔5s钟做一次
//        env.enableCheckpointing(5000L);
//        //2.2指定ck的一致性语义(精准一次性消费)
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        //2.3设置任务关闭的时候保留最后一次的ck数据
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //2.4指定从ck自动重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
//        //2.5设置状态后端
//        env.setStateBackend(new FsStateBackend("hadoop102:8020/flinkCDC"));
//        //2.6设置访问HDFS的用户名
//        System.setProperty("HADOOP_USER_NAME", "atguigu");
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall_flink")
                .username("root")
                .password("123456")
                .startupOptions(StartupOptions.initial())
                //最初测试序列化用的是:StringDebeziumDeserializationSchema()这个类
                //但是格式太复杂,所以序列化需要自定义一下
                //.deserializer(new StringDebeziumDeserializationSchema())
                .deserializer(new MyCustomDeserializer())
                .build();

        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        //打印数据
        streamSource.print();
        env.execute();


    }
}

