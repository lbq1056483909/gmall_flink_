package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName BaseLogApp
 * @Description TODO
 * @Author 10564
 * @Date 2022/2/8 18:28
 * @Version 1.0
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //todo 1.获取json的执行环境
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
        //todo 2.消费kafka ods_base_log(就是将ods中数据读进来,准备处理成dwd层数据)
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource("ods_base_log", "base_log_app_210826"));

        //todo 3.转换为JSON对象(并且过滤出脏数据)
        OutputTag<String> dirtyTag = new OutputTag<String>("dirty") {
        };
//        SingleOutputStreamOperator<JSONObject> jsonObject = kafkaDS.map(new MapFunction<String, JSONObject>() {
//            @Override
//            public JSONObject map(String value) throws Exception {
//                return JSON.parseObject(value);
//            }
//        });
        SingleOutputStreamOperator<JSONObject> jsonObject = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    //将kafkaguo'lai
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (JSONException e) {
                    e.printStackTrace();
                    ctx.output(dirtyTag, value);
                }
            }
        });

        //todo 4.按照Mid分组(状态编程是每一个不同的key进行状态编程,这边看的是新老访客,所以键是mid)
        KeyedStream<JSONObject, String> keyedStream = jsonObject.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });

        //todo 5.新老用户校验,状态编程
        //1.先判断新来的数据is_new 是不是1 ,如果不是,就直接是老用户返回就行
        //2.如果是1,再判断状态里面的值是否为null,为null则是新用户,修改value状态
        //3.如果是1,状态不是null,则为老用户,修改一下value值的is_new
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            //todo 1.定义状态,这个状态只是保留一下is_new的一个具体值,所以用string
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //todo 初始化状态
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("is-new", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                //1.获取is_new标记,getJSONobject先获取里面的json对象,然后再获取这个json的对应字段
                String isNew = value.getJSONObject("common").getString("is_new");
                //2.判断标记是否为"1"
                //如果为1,则再判断历史状态,如果不为1,则直接返回值为老用户
                //如果历史状态不为null,则已经是老用户,修改值is_mid为0
                //如果历史状态为null,则就是新用户,更新状态不为不为null
                if ("1".equals(isNew)) {
                    //3.取出状态数据
                    String isNewState = valueState.value();
                    //4.判断状态是否为null
                    //如果是null,就说明是新用户,更新状态
                    if (isNewState == null) {
                        valueState.update("old");

                        //如果不是null,表示已经是老用户,将数据的is_new更新为0
                    } else {
                        //更新数据
                        value.getJSONObject("common").put("is_new", "0");
                    }
                }
                return value;
            }
        });

        //todo 6.分流 侧输出流 主流 :页面日志  侧输出流:启动 曝光
        //侧输出流中需要
        OutputTag<String> outputTagstart = new OutputTag<String>("start") {
        };
        OutputTag<String> outputTagdisplay = new OutputTag<String>("display") {
        };

        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                //获取启动数据
                String start = value.getString("start");
                if (start != null) {
                    //将数据写入启动日志侧输出流中(因为前面制定了输出时用string输出)
                    ctx.output(outputTagstart, value.toJSONString());
                } else {
                    //将数据写入页面日志主流
                    out.collect(value.toJSONString());
                    //获取曝光数据
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        //获取page页面的pageId
                        String pageId = value.getJSONObject("page").getString("page_id");
                        Long ts = value.getLong("ts");
                        //因为曝光日志是一个数组,所以需要遍历这个曝光日志写出
                        for (int i = 0; i < displays.size(); i++) {
                            //遍历JSON数组中每一个json
                            JSONObject display = displays.getJSONObject(i);
                            display.put("page_id", pageId);
                            display.put("ts", ts);
                            ctx.output(outputTagdisplay, display.toJSONString());
                        }
                    }
                }
            }
        });
        //todo 7.提取侧输出流中的数据并写出数据到对应的kafka主题中
        DataStream<String> startDS = pageDS.getSideOutput(outputTagstart);
        DataStream<String> displayDS = pageDS.getSideOutput(outputTagdisplay);
        //将对应的侧输出流写入对应主题
        pageDS.addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
        startDS.addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));

        pageDS.print("Page>>>>>>>>");
        startDS.print("start>>>>>>");
        displayDS.print("Display");

        //todo 8.启动任务
        env.execute("BaseLogApp");


    }
}

