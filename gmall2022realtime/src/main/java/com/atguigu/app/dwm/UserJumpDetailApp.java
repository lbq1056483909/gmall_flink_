package com.atguigu.app.dwm;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //1.todo 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//生产环境与kafka主题的分区数保持一致
        //CK
        //        env.setStateBackend(new FsStateBackend("hdfs://"));
        //        env.enableCheckpointing(5000L);
        //        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //        env.getCheckpointConfig().setCheckpointInterval(10000L);
        //2. todo 读取kafka页面日志主题数据创建流
        //输入页面日志主题
        String sourceTopic = "dwd_page_log";
        //消费者组
        String groupId = "user_jump_detail_app_210826";
        //输出的主题
        String sinkTopic = "dwm_user_jump_detail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(sourceTopic, groupId));
        //3.todo 将数据转换为json对象 同时提取里面的事件时间生产watermark
        SingleOutputStreamOperator<JSONObject> map = kafkaDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSON.parseObject(value);
            }
        });
        SingleOutputStreamOperator<JSONObject> jsonObjDS = map.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(14))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        //以日志中的ts作为时间戳
                        return element.getLong("ts");
                    }
                })
        );

        //4.todo 按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });

        //5.todo 定义模式序列(lastpageid连续两个都为null才可以)
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            //将第一条数据last_page_id 不为null的数据过滤掉
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
            //next表示的是连续两条数据
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
                //表示在事件时间10s内的数据将会被匹配,超过10s,将会被丢弃
            }
            //超时时间为10s
        }).within(Time.seconds(10));

        //6.todo 将模式序列作用于流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        //7.todo 提取匹配上的以及超时事件
        OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("timeOut"){};
        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(outputTag,
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    //超时事件流
                    public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                        return map.get("start").get(0);
                    }
                }, new PatternSelectFunction<JSONObject, JSONObject>() {
                    //正常事件流
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                        return map.get("start").get(0);
                    }
                });
        //匹配上的超时时间
        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(outputTag);
        //8.todo 合并两个流
        selectDS.print("Select>>>>>>>");
        timeOutDS.print("TimeOut>>>>>>");
        DataStream<JSONObject> unionDS = selectDS.union(timeOutDS);

        //9.todo 输出数据到kafka
        unionDS.map(JSON::toString).addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        //10.todo 启动任务
        env.execute();
    }
}

