package viettel.spark.sql.log.dbfactory;

import viettel.spark.sql.log.connection.ConnectionPool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class SparkLogging {
    /**
     * Tao mot ban ghi log khi co mot session dang nhap.
     * Table: logger
     * @param sessionId
     * @param statement
     * @param state
     * @throws SQLException
     */

    private static String schema = "VBI_DWH";
    private static String table = "LOGGER_SPARK2_HOT_2";

    public void addLogger(String sparkApp, String sessionId, String groupId, String ipAddress, String statement, String state) throws SQLException{
        String SQL = "INSERT INTO " + schema + "." + table + " (SPARK_APP, SESSION_ID, GROUP_ID, IP_ADDRESS, STATEMENT, STATE, " +
                "DETAIL, DURATION, DURATION_TIME, END_AT)" + "VALUES (?, ?, ?, ?, ?, ?, null, null, null, null)";

        try(Connection connection = ConnectionPool.getDataSource().getConnection();
            PreparedStatement prep = connection.prepareStatement(SQL)){
            prep.setString(1, sparkApp);
            prep.setString(2, sessionId);
            prep.setString(3, groupId);
            prep.setString(4, ipAddress);
            prep.setString(5, statement);
            prep.setString(6, state);
            prep.execute();
        }
    }

    /**
     * Cap nhat ban ghi log (thong thuong cho su kien session bi cancel).
     * Table: logger
     * @param sessionId
     * @param state
     * @param endAt
     * @param detail
     * @throws SQLException
     */
    public void updateLogger(String groupId, String sessionId, String state, long duration, String duration_time, Timestamp endAt, String detail, String error) throws SQLException {
        String SQL = "UPDATE " + schema + "." + table + " SET DURATION = ?, DURATION_TIME = ?, STATE = ?, END_AT = ?, DETAIL = ?, ERROR = ? where GROUP_ID = ? AND SESSION_ID = ?";

        try(Connection connection = ConnectionPool.getDataSource().getConnection();
            PreparedStatement prep = connection.prepareStatement(SQL)){
            prep.setLong(1, duration);
            prep.setString(2, duration_time);
            prep.setString(3, state);
            prep.setTimestamp(4, endAt);
            prep.setString(5, detail);
            prep.setString(6, error);
            prep.setString(7, groupId);
            prep.setString(8, sessionId);
            prep.execute();
        }
    }
    
}
