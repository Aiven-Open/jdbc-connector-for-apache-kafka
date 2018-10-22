package io.confluent.connect.jdbc.util;

import java.time.DateTimeException;
import java.time.ZoneId;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class TimeZoneValidator implements ConfigDef.Validator {

  public static final TimeZoneValidator INSTANCE = new TimeZoneValidator();

  @Override
  public void ensureValid(String name, Object value) {
    if (value != null) {
      try {
        ZoneId.of(value.toString());
      } catch (DateTimeException e) {
        throw new ConfigException(name, value, "Invalid time zone identifier");
      }
    }
  }
}
