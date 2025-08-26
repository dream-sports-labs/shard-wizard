package com.dream11.shardwizard.metric.event;

import com.dream11.shardwizard.constant.RdsCluster;
import com.dream11.shardwizard.metric.ObservabilityServiceFactory;
import com.dream11.shardwizard.model.ObservabilityEvent;

public class DatabaseEventRecorder {
  private static final DatabaseEventRecorder INSTANCE = new DatabaseEventRecorder();

  private DatabaseEventRecorder() {}

  public static DatabaseEventRecorder getInstance() {
    return INSTANCE;
  }

  public void recordEvent(ObservabilityEvent event) {
    ObservabilityServiceFactory.getInstance().recordEvent(event);
  }

  public void recordSuccess(String eventName) {
    recordEvent(
        DatabaseEventBuilder.create()
            .eventName(eventName)
            .severity(ObservabilityEvent.EventSeverity.INFO)
            .build());
  }

  public void recordSuccess(String eventName, RdsCluster cluster, String query) {
    recordEvent(
        DatabaseEventBuilder.create()
            .eventName(eventName)
            .severity(ObservabilityEvent.EventSeverity.INFO)
            .cluster(cluster)
            .query(query)
            .build());
  }

  public void recordError(String eventName, Throwable error) {
    recordEvent(DatabaseEventBuilder.create().eventName(eventName).error(error).build());
  }

  public void recordError(String eventName, RdsCluster cluster, String query, Throwable error) {
    recordEvent(
        DatabaseEventBuilder.create()
            .eventName(eventName)
            .cluster(cluster)
            .query(query)
            .error(error)
            .build());
  }
}
