package com.ifchange.flink.mysql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;


/**
 * MYSQL数据库底层封装
 *
 * @author Administrator
 */
public class Mysql implements Serializable {

    private PreparedStatement pstmt;

    private Connection conn;

    private ResultSet rs;

    private volatile boolean isBusy = false;

    private String tableName = "mysql";

    private String dbUsername;

    private String passWord;

    private String dbHost = "127.0.0.1";

    private int dbPort = 3306;

    private String enCoding = "UTF-8";

    private static final Logger LOG = LoggerFactory.getLogger(Mysql.class);

    private static final DateTimeFormatter DTF = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public Mysql(String username, String password) throws Exception {
        try {
            dbUsername = username;
            passWord = password;
            createConn();
        } catch (Exception e) {
            throw new Exception(e);
        }
    }

    public Mysql(String username, String password, String dbName) throws Exception {
        try {
            tableName = dbName;
            dbUsername = username;
            passWord = password;
            createConn();
        } catch (Exception e) {
            throw new Exception(e);
        }
    }

    public Mysql(String username, String password, String dbName, String host) throws Exception {
        try {
            tableName = dbName;
            dbUsername = username;
            dbHost = host;
            passWord = password;
            createConn();
        } catch (Exception e) {
            throw new Exception(e);
        }
    }

    public Mysql(String username, String password, String dbName, String host, int port)
        throws Exception {
        try {
            tableName = dbName;
            dbUsername = username;
            dbHost = host;
            dbPort = port;
            passWord = password;
            createConn();
        } catch (Exception e) {
            throw new Exception(e);
        }
    }

    public Mysql(String username, String password, String dbName, String host, int port,
                 String encoding) throws Exception {
        try {
            tableName = dbName;
            dbUsername = username;
            dbHost = host;
            dbPort = port;
            passWord = password;
            enCoding = encoding;
            createConn();
        } catch (Exception e) {
            throw new Exception(e);
        }
    }

    public String getTableName() {
        return tableName;
    }

    public String getHost() {
        return dbHost;
    }

    private void createConn() throws Exception {
        try {
            conn = DBConnection
                .getDBConnection(dbUsername, passWord, tableName, dbHost, dbPort, enCoding);
        } catch (Exception e) {
            throw new Exception(e);
        }
    }

    public boolean updateOrAdd(String[] column, int[] type, String sql) throws SQLException {
        if (!setPstmtParam(column, type, sql)) {
            return false;
        }
        boolean flag = pstmt.executeUpdate() > 0;
        close();
        return flag;
    }

    public DataTable getResultData(String[] column, int[] type, String sql) throws SQLException {
        DataTable dt = new DataTable();

        List<HashMap<String, String>> list = new ArrayList<>();

        if (!setPstmtParam(column, type, sql)) {
            return null;
        }
        rs = pstmt.executeQuery();
        ResultSetMetaData rsmd = rs.getMetaData();//取数据库的列名
        int numberOfColumns = rsmd.getColumnCount();
        while (rs.next()) {
            HashMap<String, String> rsTree = new HashMap<String, String>();
            for (int r = 1; r < numberOfColumns + 1; r++) {
                rsTree.put(rsmd.getColumnName(r), rs.getObject(r).toString());
            }
            list.add(rsTree);
        }
        close();
        dt.setDataTable(list);
        return dt;
    }

    private boolean setPstmtParam(String[] coulmn, int[] type, String sql)
        throws NumberFormatException, SQLException {
        if (sql == null) {
            return false;
        }
        pstmt = conn.prepareStatement(sql);
        if (coulmn != null && type != null && coulmn.length != 0 && type.length != 0) {
            for (int i = 0; i < type.length; i++) {
                switch (type[i]) {
                    case Types.INTEGER:
                        pstmt.setInt(i + 1, Integer.parseInt(coulmn[i]));
                        break;
                    case Types.SMALLINT:
                        pstmt.setInt(i + 1, Integer.parseInt(coulmn[i]));
                        break;
                    case Types.BOOLEAN:
                        pstmt.setBoolean(i + 1, Boolean.parseBoolean(coulmn[i]));
                        break;
                    case Types.CHAR:
                        pstmt.setString(i + 1, coulmn[i]);
                        break;
                    case Types.DOUBLE:
                        pstmt.setDouble(i + 1, Double.parseDouble(coulmn[i]));
                        break;
                    case Types.FLOAT:
                        pstmt.setFloat(i + 1, Float.parseFloat(coulmn[i]));
                        break;
                    case Types.BIGINT:
                        pstmt.setLong(i + 1, Long.parseLong(coulmn[i]));
                        break;
                    default:
                        break;
                }
            }
        }
        return true;
    }

    public void close() throws SQLException {
        if (rs != null) {
            rs.close();
            rs = null;
        }
        if (pstmt != null) {
            pstmt.close();
            pstmt = null;
        }
        if (conn != null) {
            conn.close();
        }
    }

    public List<Map<String, Object>> executeQuery(String sql) throws SQLException {
        busy();
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        while (true) {
            try {
                pstmt = conn.prepareStatement(sql);
                rs = pstmt.executeQuery();
                ResultSetMetaData rsmd = rs.getMetaData();//取数据库的列名
                int numberOfColumns = rsmd.getColumnCount();
                while (rs.next()) {
                    HashMap<String, Object> rsTree = new HashMap<>();
                    for (int r = 1; r < numberOfColumns + 1; r++) {
                        rsTree.put(rsmd.getColumnLabel(r), rs.getObject(r));
                    }
                    list.add(rsTree);
                }
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
            }
        }
        return list;
    }



    /**
     * insert 返回主键id
     */
    public int executeInsert(String sql) throws SQLException {
        busy();
        int id = 0;
        while (true) {
            try {
                pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                pstmt.executeUpdate();
                ResultSet res = pstmt.getGeneratedKeys();
                while (res.next()) {
                    id = res.getInt(1);
                }
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
            }
        }
        return id;
    }

    public void executeBatchInsert(String sql, Iterator<Tuple2<String, Integer>> iterator) throws SQLException {
        busy();
        conn.setAutoCommit(false);
        pstmt = conn.prepareStatement(sql);
        while (iterator.hasNext()) {
            Tuple2<String, Integer> tuple2 = iterator.next();
            try {
                pstmt.setString(1, tuple2.f0);
                pstmt.setInt(2, tuple2.f1);
                pstmt.addBatch();
            } catch (SQLException ex) {
                processException(ex);
            }
        }
        pstmt.executeBatch();
        conn.commit();
        free();
    }


    public boolean execute(String sql) throws SQLException {
        busy();
        boolean result = true;
        while (true) {
            try {
                pstmt = conn.prepareStatement(sql);
                result = pstmt.execute(sql);
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
                result = false;
            }
        }
        return result;
    }

    public List<String> listTable() throws SQLException {
        busy();
        List<String> tables = new ArrayList<String>();
        while (true) {
            try {
                pstmt = conn.prepareStatement(String.format("SHOW TABLES FROM `%s`", tableName));
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    tables.add(rs.getObject("Tables_in_".concat(tableName)).toString());
                }
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
            }
        }
        return tables;
    }

//    public List<String> listDatabase() throws SQLException {
//        busy();
//        List<String> databases = new ArrayList<String>();
//        while (true) {
//            try {
//                pstmt = conn.prepareStatement("show databases");
//                rs = pstmt.executeQuery();
//                while (rs.next()) {
//                    databases.add(rs.getObject("Database").toString());
//                }
//                free();
//                break;
//            } catch (SQLException ex) {
//                processException(ex);
//            }
//        }
//        return databases;
//    }

    public void busy() {
        isBusy = true;
    }

    public void free() throws SQLException {
        isBusy = false;
        if (pstmt != null) {
            pstmt.close();
            pstmt = null;
        }
        if (rs != null) {
            rs.close();
            rs = null;
        }
    }

    public boolean isbusy() {
        return isBusy;
    }

    public void setTableName(String table) throws SQLException {
        tableName = table;
        execute("use `" + table + "`");
    }

    public boolean isValid() {
        try {
            return conn.isValid(3000);
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean isSupportsBatchUpdates() throws SQLException {
        //conn是Connection的类型
        DatabaseMetaData dbmd = conn.getMetaData();
        //为true则意味着该数据是支持批量更新的
        return dbmd.supportsBatchUpdates();
    }

    public Connection getConn() {
        reConnect();
        return conn;
    }

    private void reConnect() {
        while (true) {
            try {
                free();
                if (!conn.isValid(3000)) {
                    close();
                    try {
                        createConn();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                break;
            } catch (SQLException ex) {
                ex.printStackTrace();
                try {
                    Thread.currentThread().sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private boolean processException(SQLException ex) throws SQLException {
        int err_code = ex.getErrorCode();
        String err_msg = ex.getMessage();
        System.out.println("_._._._._._._._._._" + err_code + ":" + err_msg);
        //0 Communications link failure The last packet successfully received from the server was 9,733,566 milliseconds ago.
        if (!(err_code != 2013 && err_code != 2006 && err_code != 1053 && !err_msg
            .contains("No operations allowed after connection closed") && !err_msg
            .contains("The last packet successfully received from"))) {
            ex.printStackTrace();
            reConnect();
        } else {
            throw new SQLException(ex.getMessage(), ex.getSQLState(), err_code);
        }
        return true;
    }

    /**
     * insert into `active_users`(`tid`,`uid`,`visit_url`,`visit_time`,`created_at`,`updated_at`) values(?,?,?,?,?,?)
     */
    public int saveActiveUsers(String sql, long tid, long uid, String visitUrl, String visitTime) throws SQLException {
        busy();
        int result;
        while (true) {
            try {
                pstmt = conn.prepareStatement(sql);
                pstmt.setLong(1, tid);
                pstmt.setLong(2, uid);
                pstmt.setString(3, visitUrl);
                pstmt.setTimestamp(4, Timestamp.valueOf(LocalDateTime.parse(visitTime, DTF)));
                pstmt.setTimestamp(5, Timestamp.valueOf(LocalDateTime.now()));
                pstmt.setTimestamp(6, Timestamp.valueOf(LocalDateTime.now()));
                result = pstmt.executeUpdate();
                free();
                break;
            } catch (SQLException ex) {
                processException(ex);
                throw new SQLException(ex);
            }
        }
        return result;
    }

}
