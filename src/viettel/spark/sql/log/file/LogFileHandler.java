package viettel.spark.sql.log.file;


import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.json.JSONException;
import org.json.JSONObject;

public class LogFileHandler {

  static{
    PropertyConfigurator.configure("../etc/log4j.properties");
  }
  private static final Logger loggerResult = Logger.getLogger("spark_sql_result_logs");
  private static final Logger loggerQuery = Logger.getLogger("spark_sql_logs");

  public void writeResultSparkSQLToLogFile(
      String sparkApp,
      String sessionId,
      String groupId,
      String ip,
      String statement,
      String state,
      String detail,
      String error,
      String duration,
      String durationFormatted,
      String startTime,
      String finishTimeStamp) throws JSONException {
//    Logger logger = Logger.getLogger("spark_sql_result_logs");
    loggerResult.info(convertResultSparkSQLObjToJson(sparkApp, sessionId, groupId, ip, statement, state, detail, error,
      duration, durationFormatted, startTime, finishTimeStamp));
  }

  public void writeQuerySparkSQLToLogFile(
    String sparkApp,
    String sessionId,
    String groupId,
    String ip,
    String statement,
    String startTime) throws JSONException {
//    Logger logger = Logger.getLogger("spark_sql_logs");
    loggerQuery.info(convertSparkSQLQueryObjToJson(sparkApp, sessionId, groupId, ip, statement, startTime));
  }

  private JSONObject convertResultSparkSQLObjToJson(
      String sparkApp,
      String sessionId,
      String groupId,
      String ip,
      String statement,
      String state,
      String plan,
      String error,
      String duration,
      String durationFormatted,
      String startTime,
      String finishTimeStamp) throws JSONException {

    ResultSparkSQLObj log = new ResultSparkSQLObj(sparkApp, sessionId, groupId, ip, removeEndOfLine(statement), state,
      plan, error, duration, durationFormatted, startTime, finishTimeStamp);

    return new JSONObject(log);
  }

  private JSONObject convertSparkSQLQueryObjToJson(
      String sparkApp,
      String sessionId,
      String groupId,
      String ip,
      String statement,
      String startTime) throws JSONException {

    SparkSQLQueryObj log = new SparkSQLQueryObj(sparkApp, sessionId, groupId, ip, statement, startTime);

    return new JSONObject(log);
  }

  private String removeEndOfLine(String statement){
    return statement.trim().replaceAll("(\n|\r|\t)", " ").replaceAll("\\s{2,}", " ");
  }

}
