package io.confluent.connect.jdbc.util;

import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

public class TimeZoneValidatorTest {

  @Test
  public void testAccuracy() {
    String[] validTimeZones = new String[]{
        "Europe/Vienna",
        "Asia/Tokyo",
        "America/Los_Angeles",
        "UTC",
        "GMT+01:00",
        "UTC"
    };

    Stream.of(validTimeZones)
        .forEach(timeZone -> TimeZoneValidator.INSTANCE.ensureValid("db.timezone", timeZone));
  }

  @Test
  public void testTimeZoneNotSpecified() {
    TimeZoneValidator.INSTANCE.ensureValid("db.timezone", null);
  }

  @Test(expected = ConfigException.class)
  public void testInvalidTimeZone() {
    TimeZoneValidator.INSTANCE.ensureValid("db.timezone", "invalid");
  }

  @Test(expected = ConfigException.class)
  public void testEmptyTimeZone() {
    TimeZoneValidator.INSTANCE.ensureValid("db.timezone", "");
  }
}
