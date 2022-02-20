package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.atguigu.app.bean.OrderWide;
import com.atguigu.app.bean.PaymentInfo;
import com.atguigu.app.bean.PaymentWide;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @ClassName PaymentWideApp
 * @Description TODO
 * @Author 10564
 * @Date 2022/2/16 1:23
 * @Version 1.0
 */

public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        //todo 1.获取执行环境
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

        //todo 2.读取kafka订单宽表和支付表主题创建流
        String groupId = "payment_wide_group_210826";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        DataStreamSource<String> orderWideStrDS = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId));
        DataStreamSource<String> payStrDS = env.addSource(MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId));

        //todo 3.将数据转换为对应的Javabean对象,同时提取时间戳生成watermark
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideStrDS.map(line -> JSON.parseObject(line, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                    @Override
                    public long extractTimestamp(OrderWide orderWide, long recordTimestamp) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            return sdf.parse(orderWide.getCreate_time()).getTime();
                        } catch (ParseException e) {
                            return Long.MIN_VALUE;
                        }
                    }
                }));
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = payStrDS.map(line -> JSON.parseObject(line, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                    @Override
                    public long extractTimestamp(PaymentInfo paymentInfo, long recordTimestamp) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            return sdf.parse(paymentInfo.getCreate_time()).getTime();
                        } catch (ParseException e) {
                            return Long.MIN_VALUE;
                        }
                    }
                }));

        //todo 4.双流join
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDS.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });


        //todo 5.将数据写出到kafka
        paymentWideDS.print("paymentWideDS>>>>>>>>>>>>");
        paymentWideDS.map(JSON::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));

        //todo 6.启动任务
        env.execute("PaymentWideApp");
    }
}
