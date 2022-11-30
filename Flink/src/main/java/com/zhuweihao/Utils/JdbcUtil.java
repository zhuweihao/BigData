package com.zhuweihao.Utils;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.sql.*;
import java.util.Properties;

/**
 * @Author zhuweihao
 * @Date 2022/11/30 10:58
 * @Description com.zhuweihao.Utils
 */
public class JdbcUtil {

    private static String DATABASE_DRIVER;
    private static String DATABASE_URL;
    private static String DATABASE_USER;
    private static String DATABASE_PASSWORD;

    // 初始化读取到相关信息
    static {
        try {
            Properties prop = new Properties();
            ClassLoader classLoader = JdbcUtil.class.getClassLoader();
            URL res = classLoader.getResource("db.properties");
            String path = res.getPath();
            prop.load(new FileReader(path));
            DATABASE_URL = prop.getProperty("DATABASE_URL");
            DATABASE_USER = prop.getProperty("DATABASE_USER");
            DATABASE_PASSWORD = prop.getProperty("DATABASE_PASSWORD");
            DATABASE_DRIVER = prop.getProperty("DATABASE_DRIVER");
            // 加载驱动
            Class.forName(DATABASE_DRIVER);
        } catch (ClassNotFoundException | FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取连接
     *
     * @return Connection对象
     * @throws SQLException
     */
    public static Connection getConnection() {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(DATABASE_URL, DATABASE_USER, DATABASE_PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 关闭连接
     *
     * @param connection
     * @param statement
     * @param resultSet
     * @throws SQLException
     */
    public static void closeConnection(Connection connection, Statement statement, ResultSet resultSet) {
        try {
            //关闭connection
            if (connection != null) {
                connection.close();
            }
            //关闭statement
            if (statement != null) {
                statement.close();
            }
            //关闭结果集
            if (resultSet != null) {
                resultSet.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭连接
     *
     * @param connection
     * @param statement
     * @throws SQLException
     */
    public static void closeConnection(Connection connection, Statement statement) {
        try {
            //关闭connection
            if (connection != null) {
                connection.close();
            }
            //关闭statement
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 执行 增、删、改
     *
     * @param connection 连接对象
     * @param sql        sql语句
     * @param objects    sql中的变量
     * @return 执行成功返回true，否则返回false
     * @throws SQLException
     */
    public static boolean executeUpdate(Connection connection, String sql, Object... objects) {
        int flag = 0;
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            for (int i = 0; i < objects.length; i++) {
                preparedStatement.setObject(i + 1, objects[i]);
            }
            flag = preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if (flag > 0) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 执行 查
     *
     * @param connection 连接对象
     * @param sql        查询语句
     * @param objects    查询语句中的变量
     * @return 返回结果集resultSet
     * @throws SQLException
     */
    public static ResultSet executeQuery(Connection connection, String sql, Object... objects) {
        ResultSet resultSet = null;
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            for (int i = 0; i < objects.length; i++) {
                preparedStatement.setObject(i + 1, objects[i]);
            }
            resultSet = preparedStatement.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return resultSet;
    }

}
