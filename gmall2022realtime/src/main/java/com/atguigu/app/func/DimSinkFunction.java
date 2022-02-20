package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;


/**
 * @ClassName DimSinkFunction
 * @Description TODO
 * @Author 10564
 * @Date 2022/2/11 11:50
 * @Version 1.0
 */

//通过自定义sink将数据传到phoenix,传入的数据类型是hbase侧输出流中的,从中查看类型可以看出是
public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        //创建phoenix连接,放在open方法里面,只需要测试一次就行
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //这是hbase流里面的数据,看一下写入到phoenix里面需要哪些内容,需要写入的数据,以及写入表格的名称,
    // 由于在前面写出流的时候,没有加上sinktable,所以回去加上,方便这里使用
    //value:{"database":"gmall-210826-flink","tableName":"base_trademark","before":{},"after":{"id":"...","tm_name":"...","logo_url":"..."},"type":"insert","sinkTable":"dim_base_trademark"}
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        try {
            //1.构建sql语句(单独将执行的sql语句单独拿出来写一个方法清楚一些,然后方法里面填入所需要的参数)
            //因为这边的数据都是固定给过来的,直接执行整个sql就行,不需要???
            String sql = genSql(value.getString("sinkTable"), value.getJSONObject("after"));
            System.out.println(sql);
            //2.编译sql
            preparedStatement = connection.prepareStatement(sql);
            //todo 此步骤为后续热点数据缓存在redis中,已经缓存的数据如果有变化,有直接删除redis数据
            //先判断进来的数据为update类型,因为如果是insert类型,redis里面肯定也没有缓存
            if ("update".equals(value.getString("type"))) {
                //可以根据value获取到tablename(传到phoenix中的字段)和pk(主键)
                DimUtil.deleteRedisDimInfo(
                        //表名
                        value.getString("sinkTable").toUpperCase(),
                        //数据的主键字段在after数据里面
                        value.getJSONObject("after").getString("id")
                );
            }


            //3.执行写入数据操作
            preparedStatement.execute();
            //提交这次操作
            connection.commit();

            preparedStatement.close();
        } catch (SQLException e) {
            System.out.println("插入数据失败!");
            e.printStackTrace();
        } finally {
            //4.释放资源
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    //构建sql语句:upsert into db.tn(aa,bb,cc) value('..','...','...')
    private String genSql(String sinkTable, JSONObject after) {
        //{"id":"...","tm_name":"..."}
        //将json格式的当作map来处理
        Set<String> collumns = after.keySet();
        Collection<Object> values = after.values();
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(collumns, ",") + ")values('" +
                StringUtils.join(values, "','") + "')";


    }
}
