package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
//连接phoenix工具类
//创建连接各种数据库的工具类
public class JDBCUtil {
    //1.连接参数  2.sql语句  3.T的具体类型  4.转为驼峰格式
    public static <T> List<T> queryResult(Connection connection, String sql, Class<T> clz, boolean underScorecamel) throws InvocationTargetException, IllegalAccessException, SQLException, InstantiationException {
        //创建集合用于存放结果数据
        ArrayList<T> list = new ArrayList<>();
        //编译sql
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        //执行查询
        ResultSet resultSet = preparedStatement.executeQuery();
        //获取结果的元数据信息
        ResultSetMetaData metaData = resultSet.getMetaData();
        //获取元数据信息中列的数量
        int columnCount = metaData.getColumnCount();
        //遍历结果集,对每行数据创建T对象
        while (resultSet.next()) {
            //创建T对象
            T t = clz.newInstance();
            //遍历元数据中每一列,然后根据列数获取列名
            for (int i = 0; i < columnCount; i++) {
                //根据列数字获取列名
                String columnName = metaData.getColumnName(i + 1);
                //根据列名获取列值
                Object value = resultSet.getObject(columnName);
                //如果传入转换的boolean为true,转换为驼峰
                if (underScorecamel) {
                    //列名
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }
                //给T对象赋值(循环对每一个列:的类型,列名,列值)
                BeanUtils.setProperty(t, columnName, value);
            }
            //内循环是来获取每一行的每一列数据,最后凑成一行,
            // 外循环是获取每一行数据
            //集合中添加每一行查询到的数据
            list.add(t);
        }
        preparedStatement.close();
        //返回结果
        return list;
    }

    public static void main(String[] args) throws SQLException, IllegalAccessException, InstantiationException, InvocationTargetException, ClassNotFoundException {

        //Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        //todo 用工具类测试连接mysql
        //加载驱动
        Class.forName("com.mysql.jdbc.Driver");
        String url="jdbc:mysql://hadoop102:3306/test";
        String user="root";
        String password="123456";

        //创建连接
        Connection connection = DriverManager.getConnection(url, user, password);
        List<JSONObject> jsonObjects = queryResult(connection, "select * from emp",
                JSONObject.class, false);

        for (JSONObject jsonObject : jsonObjects) {
            System.out.println(jsonObject);
        }
        connection.close();
    }

}