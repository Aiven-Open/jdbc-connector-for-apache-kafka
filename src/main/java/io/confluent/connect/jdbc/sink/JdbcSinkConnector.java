package io.confluent.connect.jdbc.sink;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.sink.config.JdbcSinkConfig;

public final class JdbcSinkConnector extends SinkConnector {
  private static final Logger logger = LoggerFactory.getLogger(JdbcSinkConnector.class);

  private Map<String, String> configProps = null;

  public Class<? extends Task> taskClass() {
    return JdbcSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    logger.info("Setting task configurations for " + maxTasks + " workers.");
    final List<Map<String, String>> configs = Lists.newArrayList();
    for (int i = 0; i < maxTasks; ++i) {
      configs.add(configProps);
    }
    return configs;
  }

  @Override
  public void start(Map<String, String> props) {
    final Joiner.MapJoiner mapJoiner = Joiner.on(',').withKeyValueSeparator("=");
    final String propsStr = mapJoiner.join(props);
    logger.info("Starting JDBC Connector with " + propsStr);
    configProps = props;
  }

  @Override
  public void stop() {
  }

  @Override
  public ConfigDef config() {
    return JdbcSinkConfig.getConfigDef();
  }

  @Override
  public String version() {
    return getClass().getPackage().getImplementationVersion();
  }
}
