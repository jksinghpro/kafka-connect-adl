package jk.bigdata.tech;

/**
 * Created by jaskaransingh on 11/6/18.
 */

import jk.bigdata.tech.util.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AdlSinkConnector extends SinkConnector {
  private static final Logger log = LoggerFactory.getLogger
          (AdlSinkConnector.class);
  private Map<String, String> configProps;
  private AdlSinkConnectorConfig config;

  /**
   * No-arg constructor. It is instantiated by Connect framework.
   */
  public AdlSinkConnector() {
    // no-arg constructor required by Connect framework.
  }

  // Visible for testing.
  AdlSinkConnector(AdlSinkConnectorConfig config) {
    this.config = config;
  }

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    configProps = new HashMap<>(props);
    config = new AdlSinkConnectorConfig(props);
    log.info("Starting Azure data lake connector {}", config.getName());
  }

  @Override
  public Class<? extends Task> taskClass() {
    return AdlSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    Map<String, String> taskProps = new HashMap<>(configProps);
    List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
    for (int i = 0; i < maxTasks; ++i) {
      taskConfigs.add(taskProps);
    }
    return taskConfigs;
  }

  @Override
  public void stop() {
    log.info("Shutting down AZ Blob connector {}", config.getName());
  }

  @Override
  public ConfigDef config() {
    return AdlSinkConnectorConfig.getConfig();
  }

}

