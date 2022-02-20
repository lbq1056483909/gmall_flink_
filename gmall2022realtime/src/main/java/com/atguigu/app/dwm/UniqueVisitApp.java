package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

/**
 * @ClassName UniqueVisitApp
 * @Description TODO
 * @Author 10564
 * @Date 2022/2/12 1:09
 * @Version 1.0
 */
//求每天新增的用户
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境与Kafka主题的分区数保持一致
        //CK
        //        env.setStateBackend(new FsStateBackend("hdfs://"));
        //        env.enableCheckpointing(5000L);
        //        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //        env.getCheckpointConfig().setCheckpointInterval(10000L);
        //TODO 2.读取Kafka 页面日志主题数据创建流
        String groupId = "unique_visit_app_210826";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(sourceTopic, groupId));

        //todo 3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSON.parseObject(value);
            }
        });
        //todo 4.按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });

        //todo 5.使用状态编程过滤数据
        SingleOutputStreamOperator<JSONObject> filter = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> valueState;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {

                //将lastpageid的状态保留下来
                ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>("last-visit", String.class);
                //todo 因为比如一个mid超过24小时不登录,然后我们需要清除它的状态
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.hours(24))
                        //OncreateAndwrite会在写入时更新ttl,
                        //OnReadAndWrite不仅在写的时候更新状态,还会在读的时候更新状态
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                //给状态加上ttl时间
                stringValueStateDescriptor.enableTimeToLive(stateTtlConfig);


                //给这个状态一个过期的时间,比如1.6日早上8点当天首次登录,利用ttl计时,数据在1.6号8点过期,
                // 如果客户在早上7点登录,则状态也会在8点清除,在10点登录时,而会将10点的数据作为第一条数据
                valueState = getRuntimeContext().getState(stringValueStateDescriptor);
                sdf = new SimpleDateFormat("yyyy-MM-dd");

            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                //1.取出上一条页面信息
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                //2.判断上一条页面信息是否为空,为空表示是刚进入界面的时间
                if (lastPageId == null) {
                    //3.取出保存状态的数据
                    String lastvisitdt = valueState.value();
                    //4.取出数据中的日期时间
                    String curDt = sdf.format(value.getLong("ts"));
                    //5.判断数据是否保留(当旧数据状态为空时,或者数据的时间(比如今天)和中间保留的状态(比如昨天)不一致时)
                    if (lastvisitdt == null || !lastvisitdt.equals(curDt)) {
                        valueState.update(curDt);
                        return true;
                    }

                }


                return false;
            }
        });

        //todo 6.将数据写出到kafka中
        filter.print(">>>>>>>>>>");
        SingleOutputStreamOperator<String> map = filter.map(new MapFunction<JSONObject, String>() {
            @Override
            public String map(JSONObject value) throws Exception {
                return value.toJSONString();
            }
        });
        map.print();
        map.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));


        //todo 7.启动任务
        env.execute();


    }
}

