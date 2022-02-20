package com.atguigu.app.dws;

import com.atguigu.app.bean.KeywordStats;
import com.atguigu.app.udf.KeywordUDTF;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName KeywordStatsApp
 * @Description TODO
 * @Author 10564
 * @Date 2022/2/19 0:52
 * @Version 1.0
 */
public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        //todo 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //CK
        //        env.setStateBackend(new FsStateBackend("hdfs://"));
        //        env.enableCheckpointing(5000L);
        //        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //        env.getCheckpointConfig().setCheckpointInterval(10000L);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //todo 2.使用DDL方式 读取kafka数据创建表,同时提取事件时间生成watermark
        String groupId = "keyword_stats_app_210826";
        String pageViewSourceTopic = "dwd_page_log";
        //只要page界面,
        tableEnv.executeSql("create table page_log(\n" +
                "\tpage MAP<String,String>, \n" +
                "\tts bigint, \n" +
                "\trt as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss')), \n" +
                "\tWATERMARK FOR rt AS rt - INTERVAL '2' SECOND \n" +
                ") with (" + MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId) );

        //todo 3.过滤出搜索数据,获取page界面item字段这个字段里面就是汉字
        Table keyWordTable = tableEnv.sqlQuery("select \n" +
                "\tpage['item'] key_word, \n" +
                "\trt \n" +
                "from page_log \n" +
                "where page['last_page_id'] = 'search' \n" +
                "and page['item'] is not null");

        //todo 4.注册UDTF函数(注册自定义的udtf分词函数)
        tableEnv.createTemporarySystemFunction("KeywordUDTF", KeywordUDTF.class);
        //todo 5.切词
        //注册这个表
        tableEnv.createTemporaryView("key_word_table", keyWordTable);
        Table wordTable = tableEnv.sqlQuery("SELECT  \n" +
                "\trt, \n" +
                "\tkey_word  \n" +
                "FROM key_word_table, \n" +
                "LATERAL TABLE(KeywordUDTF(key_word))");

        //TODO 6.分组、开窗、聚合
        tableEnv.createTemporaryView("word_table", wordTable);
        Table resultTable = tableEnv.sqlQuery("select \n" +
                "\t'search' source,\n" +
                "\tDATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' second),'yyyy-MM-dd HH:mm:ss') stt, \n" +
                "\tDATE_FORMAT(TUMBLE_end(rt, INTERVAL '10' second),'yyyy-MM-dd HH:mm:ss') edt, \n" +
                "\tkey_word keyword, \n" +
                "\tcount(*) ct, \n" +
                "\tUNIX_TIMESTAMP()*1000 ts \n" +
                "from word_table \n" +
                "group by \n" +
                "\tkey_word, \n" +
                "\tTUMBLE(rt, INTERVAL '10' second)");

        //TODO 7.将动态表转换为流
        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(resultTable, KeywordStats.class);

        //TODO 8.将数据写出到ClickHouse
        keywordStatsDataStream.print(">>>>>>>>>>>");
        keywordStatsDataStream.addSink(
                ClickHouseUtil.<KeywordStats>getSinkFunction(
                        "insert into keyword_stats_210826(keyword,ct,source,stt,edt,ts)  " +
                                " values(?,?,?,?,?,?)"));


                //todo 9.启动任务
        env.execute();

    }
}

