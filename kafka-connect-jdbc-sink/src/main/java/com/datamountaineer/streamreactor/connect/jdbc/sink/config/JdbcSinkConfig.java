package com.datamountaineer.streamreactor.connect.jdbc.sink.config;

import io.confluent.common.config.AbstractConfig;
import io.confluent.common.config.ConfigDef;
import io.confluent.common.config.ConfigDef.Importance;
import io.confluent.common.config.ConfigDef.Type;

import java.util.Map;

/**
 * <h1>JdbcSinkConfig</h1>
 * <p>
 * Holds config, extends AbstractConfig.
 **/
public class JdbcSinkConfig extends AbstractConfig {

    public JdbcSinkConfig(Map<String, String> props) {
        super(config, props);
    }

    public final static String DATABASE_CONNECTION = "connect.jdbc.connection";
    public final static String DATABASE_CONNECTION_DOC = "Specifies the database connection";


    public final static String DATABASE_TABLE = "connect.jdbc.table";
    public final static String DATABASE_TABLE_DOC = "Specifies the target database table to insert the data.";


    public final static String DATABASE_IS_BATCHING = "connect.jdbc.sink.batching.enabled";
    public final static String DATABASE_IS_BATCHING_DOC = "Specifies if for a given sequence of SinkRecords are batched or not.\n"+
            "<true> the data insert is batched;\n"+
            "<false> for each record a sql statement is created";

    public final static String JAR_FILE = "connect.jdbc.sink.driver.jar";
    public final static String JAR_FILE_DOC = " Specifies the jar file to be loaded at runtime containing the jdbc driver";

    public final static String DRIVER_MANAGER_CLASS = "connect.jdbc.sink.driver.manager.class";
    public final static String DRIVER_MANAGER_CLASS_DOC = "Specifies the canonical class name for the driver manager.";

    public final static String FIELDS = "connect.jdbc.sink.fields";
    public final static String FIELDS_DOC = "Specifies which fields to consider when inserting the new Redis entry.\n"+
            "If is not set it will use insert all the payload fields present in the payload.\n"+
            "Field mapping is supported; this way an avro record field can be inserted into a 'mapped' column.\n"+
            "Examples:\n"+
            "* fields to be used:field1,field2,field3 \n" +
            "** fields with mapping: field1=alias1,field2,field3=alias3";

    public final static String ERROR_POLICY = "connect.jdbc.sink.error.policy";
    public final static String ERROR_POLICY_DOC = "Specifies the action to be taken if an error occurs while inserting the data.\n" +
            "There are two available options: <noop> - the error is swallowed <throw> - the error is allowed to propagate. \n" +
            "The error will be logged automatically";

    public final static ConfigDef config = new ConfigDef()
            .define(DATABASE_CONNECTION, Type.STRING, Importance.HIGH, DATABASE_CONNECTION_DOC)
            .define(DATABASE_TABLE, Type.STRING, Importance.HIGH, DATABASE_CONNECTION_DOC)
            .define(JAR_FILE, Type.STRING, Importance.HIGH, JAR_FILE_DOC)
            .define(DRIVER_MANAGER_CLASS, Type.STRING, Importance.HIGH, DRIVER_MANAGER_CLASS_DOC)
            .define(FIELDS, Type.STRING, Importance.LOW, FIELDS_DOC)
            .define(DATABASE_IS_BATCHING, Type.BOOLEAN, true, Importance.LOW, DATABASE_IS_BATCHING_DOC)
            .define(ERROR_POLICY, Type.STRING, "throw", Importance.HIGH, ERROR_POLICY_DOC);
}