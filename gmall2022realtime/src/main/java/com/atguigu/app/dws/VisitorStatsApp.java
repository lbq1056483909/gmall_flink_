package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.bean.VisitorStats;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.DateTimeUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * @ClassName VisitorStatsApp
 * @Description TODO
 * @Author 10564
 * @Date 2022/2/15 18:10
 * @Version 1.0
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        //todo 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//生产环境与kafka主题的分区数保持一致

        //CK
        // env.setStateBackend(new FsStateBackend("hdfs://"));
        // env.enableCheckpointing(5000L);
        // env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // env.getCheckpointConfig().setCheckpointTimeout(10000L);
        // env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        // env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        // env.getCheckpointConfig().setCheckpointInterval(10000L);
        //1.从dwd_page_log层数据读取pv信息,进入页面数,连续访问时长
        //2.从dwm_unique_visit层数据读取uv
        //3.从dwm_user_jump_detail层数据读取跳出率
        //todo 2.读取kafka3个主题的数据创建流
        String groupId = "visitor_stats_app_210826";
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> uniqueVisitSource = MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId);
        FlinkKafkaConsumer<String> userJumpSource = MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId);

        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> uniqueVisitDStream = env.addSource(uniqueVisitSource);
        DataStreamSource<String> userJumpDStream = env.addSource(userJumpSource);
        //todo 3.转换三个主题的数据为统计的javabean对象
        //3.1 UV数据(可以从dwm_unique_visit获取,去重后的用户数)
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUvDS = uniqueVisitDStream.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String value) throws Exception {
                //先将jsonString转换为json对象
                JSONObject jsonObject = JSON.parseObject(value);
                //取出公共字段,取出其中4个维度的值.
                JSONObject common = jsonObject.getJSONObject("common");
                //流中数据取出common字段部分,转为VisitorStats格式
                //在dwm层已经对其去重,里面的数据就是去重,然后每条都是一个用户的数据
                //将uv那一列记为1,其余行记为0
                //转换为主题宽表实体类(取出有用的字段对应的值)
                return new VisitorStats(
                        "",
                        "",
                        common.getString("vc"),
                        common.getString("ch"),
                        common.getString("ar"),
                        common.getString("is_new"),
                        1L,//uv去重后的数据
                        0L,
                        0L,
                        0L,
                        0L,
                        jsonObject.getLong("ts"));
            }
        });


        //3.2 UJ数据(跳出数据,dwm已经处理的数据)
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUjDS = userJumpDStream.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String value) throws Exception {
                //先将jsonString转换为json对象
                JSONObject jsonObject = JSON.parseObject(value);
                //取出公共字段
                JSONObject common = jsonObject.getJSONObject("common");
                return new VisitorStats(
                        "",
                        "",
                        common.getString("vc"),
                        common.getString("ch"),
                        common.getString("ar"),
                        common.getString("is_new"),
                        0L,
                        0L,
                        0L,
                        1L,//跳出次数
                        0L,
                        jsonObject.getLong("ts"));
            }
        });


        //3.3 PV数据(dwd层数据,没有处理)

        SingleOutputStreamOperator<VisitorStats> visitorStatsWithPvDS = pageViewDStream.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String value) throws Exception {
                //先将jsonString转换为json对象
                JSONObject jsonObject = JSON.parseObject(value);
                //取出公共字段
                //流中数据取出common字段部分,转为VisitorStats格式
                JSONObject common = jsonObject.getJSONObject("common");
                //将sv值初始化为0,碰到last_page_is!=null,设为1.
                long sv = 0L;
                //取出页面日志信息,方便取出里面的"last_page_id"和"during_time"
                JSONObject page = jsonObject.getJSONObject("page");
                //页面访问数(pv),进入次数(sv就是last_page_id为null的次数),持续访问时间都可以从page页面得到
                String last_page_id = page.getString("last_page_id");
                //当last_page_id为null时,然后sv这一列记为1
                if (last_page_id != null) {
                    sv = 1L;

                }
                return new VisitorStats(
                        "",
                        "",
                        common.getString("vc"),
                        common.getString("ch"),
                        common.getString("ar"),
                        common.getString("is_new"),
                        0L,
                        1L,
                        sv,
                        0L,
                        page.getLong("during_time"),//这个是页面日志信息
                        jsonObject.getLong("ts"));
            }
        });
        //todo 4.Union 3个流(union后,然后分组,再聚合才能看到结果)
        DataStream<VisitorStats> unionDS = visitorStatsWithUvDS.union(visitorStatsWithUjDS, visitorStatsWithPvDS);

        //visitorStatsWithUvDS.print("UvDS1");
        //visitorStatsWithUjDS.print("UjDS1");
        //visitorStatsWithPvDS.print("PvDS1");
        //unionDS.print("Union");

        //todo 5.提取事件时间生成watermark,乱序时间为2
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWMDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));



        //todo 6.分组开窗&聚合
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = visitorStatsWithWMDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return new Tuple4<>(value.getVc(), value.getCh(), value.getAr(), value.getIs_new());
            }
        });
        //开一个10秒的窗
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //聚合
        SingleOutputStreamOperator<VisitorStats> result = window.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());

                return value1;
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                //取出数据(聚合后只有一条数据)
                VisitorStats visitorStats = input.iterator().next();
                //获取窗口信息(这个获取的是时间戳的信息)
                long start = window.getStart();
                long end = window.getEnd();
                //设置窗口的开始与结束时间(转换为年月日时分秒)
                visitorStats.setStt(DateTimeUtil.toYMDhms(new Date(start)));
                visitorStats.setEdt(DateTimeUtil.toYMDhms(new Date(end)));
                //输出数据
                out.collect(visitorStats);
            }
        });

        //todo 7.将数据写出到ClickHouse
        result.print(">>>>>>>>>>>>>>>>>");
        result.addSink(ClickHouseUtil.getSinkFunction("insert into visitor_stats_210826 values(?,?,?,?,?,?,?,?,?,?,?,?)"));


        //todo 8.启动任务
        env.execute("VisitorStatsApp");

    }
}

