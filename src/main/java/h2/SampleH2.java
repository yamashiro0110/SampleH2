package h2;

import org.apache.commons.dbutils.DbUtils;

import java.sql.*;
import java.util.Properties;

import static java.lang.System.*;

public class SampleH2 {

    public static void main(String[] args) {
        try {
            SampleH2 h2 = new SampleH2();
            h2.init();

            out.println("現在のデータを取得");
            h2.getRecord();
            out.println();

            out.println("テーブルのデータを削除");
            h2.deleteRecord();
            out.println();

            out.println("テーブルにデータを挿入");
            h2.insertRecord(1, "hoge");
            h2.insertRecord(2, "fuga");
            h2.insertRecord(3, "piyo");
            h2.getRecord();
            out.println();

            out.println("テーブルのデータを更新");
            h2.updateRecord(1, "aaa");
            h2.updateRecord(2, "bbb");
            h2.updateRecord(3, "ccc");
            h2.getRecord();
            out.println();

            out.println("テーブルの行数を取得");
            out.println(h2.getCount());

            h2.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static final String URL = "jdbc:h2:~/workspace/gradle/h2/test";
    private Connection connection;
    private PreparedStatement preparedStatement;
    private ResultSet resultSet;

    /**
     * 初期化
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public void init() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
        Class.forName("org.h2.Driver").newInstance();
        Properties properties = new Properties();
        properties.put("user", "sa");
        connection = DriverManager.getConnection(URL, properties);
        connection.setAutoCommit(true);
    }

    /**
     * 終了処理
     * @throws SQLException
     */
    public void close() throws SQLException {
        DbUtils.closeQuietly(connection);
    }

    /**
     * 行数を取得
     * @return count
     * @throws SQLException
     */
    public int getCount() throws SQLException {
        try {
            preparedStatement = connection.prepareStatement("SELECT COUNT(1) FROM test");
            resultSet = preparedStatement.executeQuery();
            return (resultSet.next() ? resultSet.getInt(1) : 0);
        }
        finally {
            DbUtils.closeQuietly(null, preparedStatement, resultSet);
        }
    }

    /**
     * 行を取得
     * @throws SQLException
     */
    public void getRecord() throws SQLException {
        try {
            preparedStatement = connection.prepareStatement("SELECT * FROM TEST");
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                out.println(String.format(
                        "id:%s, name:%s",
                        resultSet.getInt("id"),
                        resultSet.getString("name")
                ));
            }
        }
        finally {
            DbUtils.closeQuietly(null, preparedStatement, resultSet);
        }
    }

    /**
     * 行を更新
     * @param id
     * @param name
     * @throws SQLException
     */
    public void updateRecord(int id, String name) throws SQLException {
        try {
            String sql = "UPDATE test SET name = ? WHERE id = ?";
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, name);
            preparedStatement.setInt(2, id);
            preparedStatement.executeUpdate();
        }
        finally {
            DbUtils.closeQuietly(preparedStatement);
        }
    }

    /**
     * 行を挿入
     * @param name
     * @throws SQLException
     */
    public void insertRecord(int id, String name) throws SQLException {
        try {
            preparedStatement = connection.prepareStatement("INSERT INTO test (id, name) values (?, ?)");
            preparedStatement.clearParameters();
            preparedStatement.setInt(1, id);
            preparedStatement.setString(2, name);
            preparedStatement.executeUpdate();
        }
        finally {
            DbUtils.closeQuietly(preparedStatement);
        }
    }

    /**
     * 行を削除
     * @throws SQLException
     */
    public void deleteRecord() throws SQLException {
        try {
            preparedStatement = connection.prepareStatement("DELETE FROM test");
            preparedStatement.executeUpdate();
        }
        finally {
            DbUtils.closeQuietly(preparedStatement);
        }
    }
}
