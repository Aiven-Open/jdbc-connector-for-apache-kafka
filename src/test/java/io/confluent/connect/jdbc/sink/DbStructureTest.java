package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DbStructureTest {

  DbStructure structure = new DbStructure(null);

  @Test
  public void testNoMissingFields() {
    assertTrue(missingFields(sinkRecords("aaa"), columns("aaa", "bbb")).isEmpty());
  }

  @Test
  public void testMissingFieldsWithSameCase() {
    assertEquals(1, missingFields(sinkRecords("aaa", "bbb"), columns("aaa")).size());
  }

  @Test
  public void testSameNamesDifferentCases() {
    assertTrue(missingFields(sinkRecords("aaa"), columns("aAa", "AaA")).isEmpty());
  }

  @Test
  public void testMissingFieldsWithDifferentCase() {
    assertTrue(missingFields(sinkRecords("aaa", "bbb"), columns("AaA", "BbB")).isEmpty());
    assertTrue(missingFields(sinkRecords("AaA", "bBb"), columns("aaa", "bbb")).isEmpty());
    assertTrue(missingFields(sinkRecords("AaA", "bBb"), columns("aAa", "BbB")).isEmpty());
  }

  private Set<SinkRecordField> missingFields(
      Collection<SinkRecordField> fields,
      Set<String> dbColumnNames
  ) {
    return structure.missingFields(fields, dbColumnNames);
  }

  static Set<String> columns(String... names) {
    return new HashSet<>(Arrays.asList(names));
  }

  static List<SinkRecordField> sinkRecords(String... names) {
    List<SinkRecordField> fields = new ArrayList<>();
    for (String n : names) {
      fields.add(field(n));
    }
    return fields;
  }

  static SinkRecordField field(String name) {
    return new SinkRecordField(Schema.STRING_SCHEMA, name, false);
  }
}
