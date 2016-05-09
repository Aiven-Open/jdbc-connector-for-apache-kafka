package com.datamountaineer.streamreactor.connect;

import com.datamountaineer.streamreactor.connect.config.PayloadFields;
import io.confluent.common.config.ConfigException;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PayloadFieldsTest {

    @Test
    public void returnsTrueAsPrimaryKey() {
        assertEquals(PayloadFields.isPrimaryKey("[a]"), true);
        assertEquals(PayloadFields.isPrimaryKey("[]"), true);
    }

    @Test
    public void returnsFalseAsPrimaryKey() {

        assertEquals(PayloadFields.isPrimaryKey("a]"), false);

        assertEquals(PayloadFields.isPrimaryKey("[a"), false);

        assertEquals(PayloadFields.isPrimaryKey("column_a"), false);
    }

    @Test
    public void shouldRemoveThePKconfigChars() {
        assertEquals(PayloadFields.removePrimaryKeyChars("[columnA]"), "columnA");
    }

    @Test(expected = ConfigException.class)
    public void shouldThrowConfigExceptionIfThePKFieldIsWhitespace() {
        PayloadFields.removePrimaryKeyChars("[  ]");
    }

    @Test(expected = ConfigException.class)
    public void shouldThrowConfigExceptionIfThePKFieldIsEmpty() {
        PayloadFields.removePrimaryKeyChars("[]");
    }

    @Test
    public void shouldTrimThePKField() {
        assertEquals(PayloadFields.removePrimaryKeyChars("[ colB ]"), "colB");
    }

    @Test
    public void shouldIncludeAllFields() {
        assertEquals(PayloadFields.from("*").getIncludeAllFields(), true);
    }

    @Test
    public void shouldBuildFieldMappingsForNonPK() {
        PayloadFields fields = PayloadFields.from("*, field2 = field2Renamed , field3=field3Renamed");
        assertEquals(fields.getIncludeAllFields(), true);

        Map<String, FieldAlias> map = fields.getFieldsMappings();
        assertEquals(map.size(), 2);
        assertTrue(map.containsKey("field2"));
        assertEquals(map.get("field2"), new FieldAlias("field2Renamed"));
        assertEquals(map.get("field3"), new FieldAlias("field3Renamed"));
    }

    @Test
    public void shouldBuildFieldMappingsForPKFields() {
        PayloadFields fields = PayloadFields.from("*, [field2 ] = field2Renamed , [ field3]=field3Renamed");
        assertEquals(fields.getIncludeAllFields(), true);

        Map<String, FieldAlias> map = fields.getFieldsMappings();
        assertEquals(map.size(), 2);
        assertTrue(map.containsKey("field2"));
        assertEquals(map.get("field2"), new FieldAlias("field2Renamed", true));
        assertEquals(map.get("field3"), new FieldAlias("field3Renamed", true));
    }
}
