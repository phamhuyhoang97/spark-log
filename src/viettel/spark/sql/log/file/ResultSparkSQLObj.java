package viettel.spark.sql.log.file;

public class ResultSparkSQLObj {
  private String sparkApp;
  private String sessionId;
  private String groupId;
  private String ip;
  private String statement;
  private String state;
  private String plan;
  private String error;
  private String duration;
  private String durationFormatted;
  private String startTime;
  private String finishTimeStamp;

  public ResultSparkSQLObj(
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
    String finishTimeStamp) {
    this.sparkApp = sparkApp;
    this.sessionId = sessionId;
    this.groupId = groupId;
    this.ip = ip;
    this.statement = statement;
    this.state = state;
    this.plan = plan;
    this.error = error;
    this.duration = duration;
    this.durationFormatted = durationFormatted;
    this.startTime = startTime;
    this.finishTimeStamp = finishTimeStamp;
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

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public String getPlan() {
    return plan;
  }

  public void setPlan(String plan) {
    this.plan = plan;
  }

  public String getError() {
    return error;
  }

  public void setError(String error) {
    this.error = error;
  }

  public String getDuration() {
    return duration;
  }

  public void setDuration(String duration) {
    this.duration = duration;
  }

  public String getDurationFormatted() {
    return durationFormatted;
  }

  public void setDurationFormatted(String durationFormatted) {
    this.durationFormatted = durationFormatted;
  }

  public String getStartTime() {
    return startTime;
  }

  public void setStartTime(String startTime) {
    this.startTime = startTime;
  }

  public String getFinishTimeStamp() {
    return finishTimeStamp;
  }

  public void setFinishTimeStamp(String finishTimeStamp) {
    this.finishTimeStamp = finishTimeStamp;
  }
}
