package com.atguigu.utils;

import com.atguigu.app.bean.TransientSink;
import com.atguigu.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.SQLException;

//使用的是jdbc工具类调用clickhouse,
//因为只写入到一个表里面,所以用clickhouse
public class ClickHouseUtil {
    //sinkfunction是它的返回类型addsink里面需要这个类型的参数,jdbcsink.sink就是返回的这个类型  T就是数据的泛型
    public static <T> SinkFunction<T> getSinkFunction(String sql) {
        return JdbcSink.sink(sql,
                new JdbcStatementBuilder<T>() {
                    //这个T t就是一行一行的数据,现在就是VisitorStats(stt=, edt=, vc=v2.1.134, ch=xiaomi, ar=530000, is_new=0, uv_ct=0, pv_ct=1, sv_ct=1, uj_ct=0, dur_sum=16141, ts=1608259639000)
                    //然后将里面每一个字段的值赋值给?.
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        try {
                            //通过反射的方式获取所有的属性
                            Class<?> clz = t.getClass();
                            //clz.getFields();
                            Field[] fields = clz.getDeclaredFields();//包含私有属性
                            //遍历属性,获取对应的值给占位符赋值
                            int offset = 0;
                            for (int i = 0; i < fields.length; i++) {
                                Field field = fields[i];
                                //获取私有的属性
                                field.setAccessible(true);
                                TransientSink transientSink = field.getAnnotation(TransientSink.class);
                                //如果有的字段注解后,不能序列化需要过滤掉
                                if (transientSink != null) {
                                    offset++;
                                    continue;
                                }
                                //反射,获取这个字段的值
                                Object value = field.get(t);
                                preparedStatement.setObject(i + 1 - offset, value);
                            }
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }
                },
                new JdbcExecutionOptions
                        .Builder()
                        .withBatchIntervalMs(1000L)
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());
    }

}

