package com.datamountaineer.streamreactor.connect.jdbc.sink.config;


import com.datamountaineer.streamreactor.connect.FieldAlias;
import io.confluent.common.config.ConfigException;
import org.junit.Test;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class JdbcSinkSettingsTest {

    @Test(expected = ConfigException.class)
    public void throwAnExceptionIfTheDbDialectIsNotValid() throws URISyntaxException {
        JdbcSinkConfig config = mock(JdbcSinkConfig.class);
        String connection = "somedbconnection";
        String table = "the_table";
        when(config.getString(JdbcSinkConfig.JAR_FILE))
                .thenReturn(Paths.get(getClass().getResource("/sqlite-jdbc-3.8.11.2.jar").toURI()).toAbsolutePath().toString());
        when(config.getString(JdbcSinkConfig.DRIVER_MANAGER_CLASS)).thenReturn("org.sqlite.JDBC");
        when(config.getString(JdbcSinkConfig.DATABASE_CONNECTION)).thenReturn(connection);
        when(config.getString(JdbcSinkConfig.DATABASE_TABLE)).thenReturn(table);
        when(config.getBoolean(JdbcSinkConfig.DATABASE_IS_BATCHING)).thenReturn(true);
        when(config.getString(JdbcSinkConfig.ERROR_POLICY)).thenReturn("NOOP");
        when(config.getString(JdbcSinkConfig.SQL_DIALECT)).thenReturn("NOT_VALID");

        JdbcSinkSettings.from(config);
    }


    @Test
    public void returnAnInstanceOfJdbcSinkSettingsFromJdbcSinkConfig() throws URISyntaxException {
        JdbcSinkConfig config = mock(JdbcSinkConfig.class);
        String connection = "somedbconnection";
        String table = "the_table";
        when(config.getString(JdbcSinkConfig.JAR_FILE))
                .thenReturn(Paths.get(getClass().getResource("/sqlite-jdbc-3.8.11.2.jar").toURI()).toAbsolutePath().toString());
        when(config.getString(JdbcSinkConfig.DRIVER_MANAGER_CLASS)).thenReturn("org.sqlite.JDBC");
        when(config.getString(JdbcSinkConfig.DATABASE_CONNECTION)).thenReturn(connection);
        when(config.getString(JdbcSinkConfig.DATABASE_TABLE)).thenReturn(table);
        when(config.getBoolean(JdbcSinkConfig.DATABASE_IS_BATCHING)).thenReturn(true);
        when(config.getString(JdbcSinkConfig.ERROR_POLICY)).thenReturn("NOOP");
        when(config.getString(JdbcSinkConfig.SQL_DIALECT)).thenReturn("NONE");

        JdbcSinkSettings settings = JdbcSinkSettings.from(config);

        assertEquals(settings.getConnection(), connection);
        assertEquals(settings.getTableName(), table);
        assertEquals(settings.getFields().getIncludeAllFields(), true);
        assertEquals(settings.getFields().getFieldsMappings().size(), 0);
    }


    @Test
    public void handleSettingsWhenSomeOfThePayloadFieldsAreSpecified() throws URISyntaxException {
        JdbcSinkConfig config = mock(JdbcSinkConfig.class);
        String connection = "somedbconnection";
        String table = "one_table";
        when(config.getString(JdbcSinkConfig.JAR_FILE))
                .thenReturn(Paths.get(getClass().getResource("/sqlite-jdbc-3.8.11.2.jar").toURI()).toAbsolutePath().toString());
        when(config.getString(JdbcSinkConfig.DRIVER_MANAGER_CLASS)).thenReturn("org.sqlite.JDBC");
        when(config.getString(JdbcSinkConfig.DATABASE_CONNECTION)).thenReturn(connection);
        when(config.getString(JdbcSinkConfig.FIELDS)).thenReturn("field1,field2=alias2,field3");
        when(config.getString(JdbcSinkConfig.DATABASE_TABLE)).thenReturn(table);
        when(config.getBoolean(JdbcSinkConfig.DATABASE_IS_BATCHING)).thenReturn(true);
        when(config.getString(JdbcSinkConfig.ERROR_POLICY)).thenReturn("NOOP");
        when(config.getString(JdbcSinkConfig.SQL_DIALECT)).thenReturn("NONE");

        JdbcSinkSettings settings = JdbcSinkSettings.from(config);

        assertEquals(settings.getConnection(), connection);
        assertEquals(settings.getTableName(), table);
        assertEquals(settings.getFields().getIncludeAllFields(), false);

        Map<String, FieldAlias> mappings = settings.getFields().getFieldsMappings();
        assertEquals(mappings.get("field1"), new FieldAlias("field1"));
        assertEquals(mappings.get("field2"), new FieldAlias("alias2"));
        assertEquals(mappings.get("field3"), new FieldAlias("field3"));
        assertEquals(mappings.size(), 3);
    }

    @Test
    public void hanldeSettingsWHenAllFieldsAreIncludedAndMappingsAreDefined() throws URISyntaxException {

        JdbcSinkConfig config = mock(JdbcSinkConfig.class);
        String connection = "somedbconnection";
        String table = "targettable";
        when(config.getString(JdbcSinkConfig.JAR_FILE))
                .thenReturn(Paths.get(getClass().getResource("/sqlite-jdbc-3.8.11.2.jar").toURI()).toAbsolutePath().toString());
        when(config.getString(JdbcSinkConfig.DRIVER_MANAGER_CLASS)).thenReturn("org.sqlite.JDBC");
        when(config.getString(JdbcSinkConfig.DATABASE_CONNECTION)).thenReturn(connection);
        when(config.getString(JdbcSinkConfig.DATABASE_TABLE)).thenReturn(table);
        when(config.getString(JdbcSinkConfig.FIELDS)).thenReturn("*,field2=alias2");
        when(config.getBoolean(JdbcSinkConfig.DATABASE_IS_BATCHING)).thenReturn(true);
        when(config.getString(JdbcSinkConfig.ERROR_POLICY)).thenReturn("NOOP");
        when(config.getString(JdbcSinkConfig.SQL_DIALECT)).thenReturn("NONE");

        JdbcSinkSettings settings = JdbcSinkSettings.from(config);

        assertEquals(settings.getConnection(), connection);
        assertEquals(settings.getFields().getIncludeAllFields(), true);
        assertEquals(settings.getTableName(), table);

        Map<String, FieldAlias> mappings = settings.getFields().getFieldsMappings();
        assertEquals(mappings.get("field2"), new FieldAlias("alias2"));
        assertEquals(mappings.size(), 1);
    }

    @Test
    public void handleConfigReturningDefaultForFields() throws URISyntaxException {

        JdbcSinkConfig config = mock(JdbcSinkConfig.class);
        String connection = "somedbconnection";
        String table = "targettable";
        when(config.getString(JdbcSinkConfig.JAR_FILE))
                .thenReturn(Paths.get(getClass().getResource("/sqlite-jdbc-3.8.11.2.jar").toURI()).toAbsolutePath().toString());
        when(config.getString(JdbcSinkConfig.DRIVER_MANAGER_CLASS)).thenReturn("org.sqlite.JDBC");
        when(config.getString(JdbcSinkConfig.DATABASE_CONNECTION)).thenReturn(connection);
        when(config.getString(JdbcSinkConfig.DATABASE_TABLE)).thenReturn(table);
        when(config.getString(JdbcSinkConfig.FIELDS)).thenReturn("*");
        when(config.getBoolean(JdbcSinkConfig.DATABASE_IS_BATCHING)).thenReturn(true);
        when(config.getString(JdbcSinkConfig.ERROR_POLICY)).thenReturn("NOOP");
        when(config.getString(JdbcSinkConfig.SQL_DIALECT)).thenReturn("NONE");

        JdbcSinkSettings settings = JdbcSinkSettings.from(config);

        assertEquals(settings.getConnection(), connection);
        assertEquals(settings.getFields().getIncludeAllFields(), true);
        assertEquals(settings.getTableName(), table);

        Map<String, FieldAlias> mappings = settings.getFields().getFieldsMappings();
        assertEquals(mappings.size(), 0);
    }
}
