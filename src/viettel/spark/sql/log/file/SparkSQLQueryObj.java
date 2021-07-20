package viettel.spark.sql.log.file;

public class SparkSQLQueryObj {
  private String sparkApp;
  private String sessionId;
  private String groupId;
  private String ip;
  private String statement;
  private String startTime;

  public SparkSQLQueryObj(String sparkApp, String sessionId, String groupId, String ip, String statement, String startTime) {
    this.sparkApp = sparkApp;
    this.sessionId = sessionId;
    this.groupId = groupId;
    this.ip = ip;
    this.statement = statement;
    this.startTime = startTime;
  }

  public String getSparkApp() {
    return sparkApp;
  }

  public void setSparkApp(String sparkApp) {
    this.sparkApp = sparkApp;
  }

  public String getSessionId() {
    return sessionId;
  }

  public void setSessionId(String sessionId) {
    this.sessionId = sessionId;
  }

  public String getGroupId() {
    return groupId;
  }

  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }

  public String getIp() {
    return ip;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }

  public String getStatement() {
    return statement;
  }

  public void setStatement(String statement) {
    this.statement = statement;
  }

  public String getStartTime() {
    return startTime;
  }

  public void setStartTime(String startTime) {
    this.startTime = startTime;
  }
}
