package com.atguigu.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * @author 10564
 * @description:
 * @param: null
 * @return:
 * @date: 2022/2/15 11:27
 */
//测试完jdbcutils后,再写DImUtil,将测试表名单独提出
//由于读取查询的速度比较慢,所以用redis缓存热点数据
public class DimUtil {
    public static JSONObject getDimInfo(Connection connection, String tableName, String pK) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
        //查询Redis
        Jedis jedis = RedisUtil.getJedis();
        //设计redis的主键,主要是保留热点数据,可以根据表+主键来查询到(表名tableName和主键Pk是通过外部参数传递进来)
        String redisKey = "DIM:" + tableName + ":" + pK;
        //将对应主键里面的缓存数据查出来
        String dimInfoStr = jedis.get(redisKey);
        //如果缓存数据为null
        if(dimInfoStr!=null){
            jedis.expire(redisKey,24*60*60);
            jedis.close();

            //return 就直接返回结果了,后面的内容不会再执行
            return JSON.parseObject(dimInfoStr);
        }

        //拼接查询语句
        String sql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id='" + pK + "'";
        //套用JDBCUtils查询
        List<JSONObject> list = JDBCUtil.queryResult(connection, sql, JSONObject.class, false);
        //获取里面第一条数据(因为按照主键来查询,里面只有一条数据)
        JSONObject dimInfo = list.get(0);
        //将数据写入Redis
        jedis.set(redisKey,dimInfo.toJSONString());
        jedis.expire(redisKey,24*60*60);
        jedis.close();
        //返回结果
        return dimInfo;
    }

    public static void deleteRedisDimInfo(String tableName,String pk){
        //如果维度表数据发生变化,将对应的缓存数据删除掉
        String redisKey = "DIM:" + tableName + ":" + pk;
        Jedis jedis = RedisUtil.getJedis();
        jedis.del(redisKey);
        jedis.close();
    }

    public static void main(String[] args) throws SQLException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        long start = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_BASE_TRADEMARK", "2"));
        long end = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_BASE_TRADEMARK", "2"));
        long end2 = System.currentTimeMillis();

        System.out.println(end - start);
        System.out.println(end2 - end);

        connection.close();
    }

}
