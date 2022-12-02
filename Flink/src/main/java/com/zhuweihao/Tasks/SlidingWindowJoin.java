package com.zhuweihao.Tasks;

import com.zhuweihao.Utils.JdbcUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @Author zhuweihao
 * @Date 2022/11/30 15:42
 * @Description com.zhuweihao.Tasks
 */
public class SlidingWindowJoin {
    public static void main(String[] args) throws SQLException {
        Connection connection = JdbcUtil.getConnection();

        String sql1 = "select s_suppkey from supplier where s_suppkey<?";
        ResultSet resultSet = JdbcUtil.executeQuery(connection, sql1, 11);
        while (resultSet.next()){
            System.out.println(resultSet.getInt("s_suppkey"));
        }
        System.out.println(resultSet);

        String sql2 = "update supplier set s_name=? where s_suppkey=?";
        boolean b = JdbcUtil.executeUpdate(connection, sql2, "Supplier#0000000030000", 3);
        System.out.println(b);

    }
}
