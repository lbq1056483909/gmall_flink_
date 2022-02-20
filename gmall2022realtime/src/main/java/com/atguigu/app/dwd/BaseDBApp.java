package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.bean.TableProcess;
import com.atguigu.app.func.DimSinkFunction;
import com.atguigu.app.func.MyCustomDeserializer;
import com.atguigu.app.func.TableProcessFunction;
import com.atguigu.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @ClassName BaseDBApp
 * @Description TODO
 * @Author 10564
 * @Date 2022/2/9 18:10
 * @Version 1.0
 */
//测试流程:
    //1.打开hdfs,zk,kf,mysql,phnoix
    //2.先在mysql配置表信息中添加一条信息
    //3.然后启动BaseDBApp这个程序,再启动ods层的程序
    //4.在mysql中模拟添加维度表数据和事实表,然后分别看进到kafka流还是hbase流中
//流程:mock->mysql(binlog开启)---flinkcdc-->ods层kafka(ods_base_db)--->(1.hbase(维度表)  2.kafka(事实表))
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //todo 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//生产环境于kafka主题的分区数保持一致
        //CK
        //     env.setStateBackend(new FsStateBackend("hdfs://"));
        //     env.enableCheckpointing(5000L);
        //     env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //     env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //     env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //     env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //     env.getCheckpointConfig().setCheckpointInterval(10000L);
        //todo 2.读取kafka ods_base_db 主题创建主流
        //将数据从kafka读进来,就是kafka的消费者,需要用kafka source
        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaUtil.getKafkaSource("ods_base_db", "base_db_app_210826"));

        //todo 3.转换为JSON对象,过滤掉空数据(删除数据)
        //先将数据转换为json对象
        SingleOutputStreamOperator<JSONObject> mapsource = dataStreamSource.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSON.parseObject(value);
            }
        });
        //过滤掉type类型为delete的数据
        SingleOutputStreamOperator<JSONObject> filtersource = mapsource.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String type = value.getString("type");
                return !"delete".equals(type);
            }
        });
        //filtersource.print("filter>>>>>>>>");
        //todo 4.使用flinkCDC读取配置信息表,并创建广播流
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
        //将cdc里面的数据转换为流
        DataStreamSource<String> dataStreamSource1 = env.addSource(sourceFunction);
        //将配置流里面的数据打印出来
        //dataStreamSource1.print("广播流");
        //将信息流创建为广播流(k:主键(拼接成string)  V:保留一行数据信息(转换成javabean))
        MapStateDescriptor<String, TableProcess> stateDescriptor = new MapStateDescriptor<>("bc-state", String.class, TableProcess.class);
        //这个就是广播流
        BroadcastStream<String> broadcastStream = dataStreamSource1.broadcast(stateDescriptor);
        //todo 5.连接主流和广播流(用主流去连接广播流)
        BroadcastConnectedStream<JSONObject, String> connect = filtersource.connect(broadcastStream);

        //todo 6.处理广播流数据以及主流数据
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase") {};
        //单独拿出来写到了func里面
        SingleOutputStreamOperator<JSONObject> kafkaDS = connect.process(new TableProcessFunction(stateDescriptor, hbaseTag));

        //todo 7.提取侧输出流数据
        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(hbaseTag);
        //todo 8.将Hbase数据写出到phoenix
        hbaseDS.print("hbase>>>>>>>");
        //将数据写入到phoenix,因为每一个的字段不一样???的个数不确定,所以要自定义一个sink
        hbaseDS.addSink(new DimSinkFunction());

        //todo 9.将Kafka数据写出
        kafkaDS.print();
        FlinkKafkaProducer<JSONObject> kafkaSink = MyKafkaUtil.getKafkaSink(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<byte[], byte[]>(
                        //主题名
                        element.getString("sinkTable"),
                        //主题里面的数据
                        element.getString("after").getBytes());
            }
        });

        kafkaDS.addSink(kafkaSink);

        //todo 10.启动任务
        env.execute("BaseDBAPP");


    }


}


