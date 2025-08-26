package com.dream11.shardwizard.metric;

import java.util.Map;

public interface MetricsAgent {
  /**
   * Records a custom event with the given name and tags
   *
   * @param eventName The name of the event to record
   * @param tags Map of key-value pairs to attach as tags to the event
   */
  void recordEventCustom(String eventName, Map<String, String> tags);

  /**
   * Records an alert event in the metrics system
   *
   * @param title The title of the alert
   * @param text The description of the alert
   * @param alertType The type of alert (ERROR, WARNING, INFO, SUCCESS)
   * @param eventTag Additional tag for the alert
   */
  void recordEvent(String title, String text, String alertType, String eventTag);

  /**
   * Records a service health check status
   *
   * @param serviceCheckName Name of the service check
   * @param status Status of the service check (OK, WARNING, CRITICAL, UNKNOWN)
   */
  void recordServiceCheck(String serviceCheckName, String status);
}
