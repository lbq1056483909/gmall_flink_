package com.atguigu.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * @ClassName TableProcessFunction
 * @Description TODO
 * @Author 10564
 * @Date 2022/2/9 20:07
 * @Version 1.0
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    //定义广播状态属性
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;


    //定义侧输出流标记属性
    private OutputTag<JSONObject> hbaseTag;


    //因为主方法中需要调用类时传递参数,所以创建一个带参的构造函数


    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor, OutputTag<JSONObject> hbaseTag) {
        this.mapStateDescriptor = mapStateDescriptor;
        this.hbaseTag = hbaseTag;
    }

    //声明连接
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        //创建连接,里面天url就行,不需要密码"jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
         connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //处理广播流数据
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        //1.解析数据,解析通过flinkcdc都过来的配置文件信息
        //数据格式也是通过反序列化器解析的
        ////value:{"database":"gmall-210826-realtime",
        //          "tableName":"table_process",
        //          "before":{},
        //          "after":{7个字段,就是配置表里面的字段,其他都是周边信息 },
        //          "type":"insert"}
        //1.先将配置信息value(json格式转换为JSON对象)
        JSONObject jsonObject = JSON.parseObject(value);
        // 将value对象after字段提取出来,然后转换成javabean格式
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        //2.根据配置信息判断Sinktype类型为Hbase数据,则建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            //在外面写这个checkTable方法,写sql时需要用到sinktable,sinkcolumns,sinkpk,sinkextend这几个参数
            System.out.println("创建phoenix表");
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }

        //3.将数据写入状态,广播出去
        //3.1提取状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //3.2广播状态中k是主键(主键是来源表名+操作类型),value是每个具体值
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        System.out.println("配置表主键"+key);
        //将配置表信息广播出去
        broadcastState.put(key, tableProcess);


    }

    //todo 当处理广播流数据,sinktable类型为hbase时,调用这个方法
    //建表方法比如:create table if not exists db.tn(id varchar primary key,name varchar,sex varchar ) xxx
    // sinkTable: 输出时创建到Phoenix的表名     sinkColumns:创建Phoenix表所有的数据列
    // sinkPk:    主键 ,创建Phoenix表时必须有一个主键        sinkExtend:
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        PreparedStatement preparedStatement = null;
        //查看jdbc时代码,先创建连接,然后写sql语句,然后执行
        //注意:1.将连接声明在方法外面  2.创建在open方法里面
        try {
            //因为Phoenix建表时必须要有一个主键,所以当sinkPk为空时,或者为null时,将sinkPk给值"id",表示主键为id
            if (sinkPk == null || "".equals(sinkPk)) {
                sinkPk = "id";
            }
            //sinkExtend为null没关系,
            if (sinkExtend == null) {
                sinkExtend = "";
            }
            //建表方法:create table if not exists db.tn(id varchar primary key,name varchar,sex varchar) xxx;
            //todo 和普通建表相同,注意主键字段
            //然后开始写sql语句,将sql语句写道StringBuilder里面,最后直接执行这个StringBuilder里面的字符串就行
            //create table if not exists db.tn(这一步写到这里,因为后面的sinkColumns是多个字段的字符串,需要切分遍历
            StringBuilder sql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            //columns是我们主动写到配置信息里面的字段,按照空格切分
            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                //遍历每一个字段
                String column = columns[i];
                //然后在sql语句后面追加每一个字段及其类型
                sql.append(column).append(" varchar");
                //判断是否为主键字段(主键字段也包含在colums里面,只不过主键有时会单独声明
                // 没有单独声明时,此程序默认是id
                if (sinkPk.equals(column)) {
                    //todo 表示这个字段等于主键时,在后面加上primary key
                    sql.append(" primary key ");
                }
                //判断是否为最后一个字段
                if (i < columns.length - 1) {
                    //当不是最后一个字段时,则需要后面加","
                    //当是最后一个字段时,则不需要加
                    sql.append(",");
                }
            }
            sql.append(")").append(sinkExtend);

            //将上面的sql语句打印出来
            System.out.println(sql);
//            //编译sql
//            preparedStatement = connection.prepareStatement(sql.toString());
//            //执行这个sql
//            preparedStatement.execute();

            //编译SQL
            preparedStatement = connection.prepareStatement(sql.toString());

            preparedStatement.execute();


        } catch (SQLException e) {
            throw new RuntimeException("建表:" + sinkTable + "失败");
        } finally {
            if (preparedStatement != null) {//preparedStatement不为null时,程序执行结束关掉,connect不需要关掉
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    //处理主流:主流的数据来自ods层,数据格式和广播流一样
    ////value:{"database":"gmall-210826-flink","tableName":"base_trademark","before":{},"after":{"id":"...","tm_name":"...","logo_url":"..."},"type":"insert"}
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //1.获取广播流数据 (这边的mapStateDescriptor是上面只是定义了,使用时通过构造器参数赋值)
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //这边是将主流中的tablename和type拼接,因为主流上的数据和配置表广播流上面主键匹配上,
        String key = value.getString("tableName") + "-" + value.getString("type");
        System.out.println("主流   :"+key);
        //如果主流中的tablename+type和广播流中的key相等,表示这个数据是配置信息中需要的数据
        TableProcess tableProcess = broadcastState.get(key);
        //System.out.println(tableProcess.toString());
        //如果!=null,表示已经匹配上
        if (tableProcess != null) {
            System.out.println("匹配上");
            //2.过滤字段(比如不要一个表的一个字段,包含在配置表中的数据才要)
            //里面的参数:1.主流中的after数据,就是整条数据   2.广播流(配置表sinkcolums)的数据字段
            filterColumn(value.getJSONObject("after"), tableProcess.getSinkColumns());

            //因为后面需要将各个表的数据发往不同的分区或者主题中,则value中需要添加sinktable这个字段
            value.put("sinkTable",tableProcess.getSinkTable());
            //3.分流 主流放kafka数据,侧输出流放hbase数据
            if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                //tableProcess是通过输入表名+输入类型得来的,然后再确认是kafka还是hbase
                System.out.println("kafka流");
                out.collect(value);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                System.out.println("hbase流");
                ctx.output(hbaseTag, value);
            }
        } else {
            System.out.println("组合key:" + key + "不存在");
        }

    }

    private void filterColumn(JSONObject after, String sinkColumns) {
        //将配置信息中columns的字段切分出来
        String[] columns = sinkColumns.split(",");
        //将数组转换为集合
        List<String> columnslist = Arrays.asList(columns);
        //注意:遇到json可以按照map来处理
        //遍历主流中的数据
        //System.out.println(columnslist);
        Set<Map.Entry<String, Object>> entries = after.entrySet();
        //调用迭代器
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        //然后遍历迭代器
        while (iterator.hasNext()) {
            //遍历after中的每一个值
            Map.Entry<String, Object> next = iterator.next();
            //然后用columns集合来判断是否包含迭代器里面值的key,没有包含的就舍弃不要
            if (!columnslist.contains(next.getKey())) {
                iterator.remove();
            }
        }
    }


}

