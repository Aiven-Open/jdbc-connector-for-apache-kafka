package com.datamountaineer.streamreactor.connect.jdbc.sink.config.writer;


import com.datamountaineer.streamreactor.connect.config.PayloadFields;
import com.datamountaineer.streamreactor.connect.jdbc.sink.JdbcDriverLoader;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.ErrorPolicyEnum;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkSettings;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.*;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class JdbcDbWriterTest {

    static {
        try {
            JdbcDriverLoader.load("org.sqlite.JDBC", Paths.get(JdbcDbWriterTest.class.getResource("/sqlite-jdbc-3.8.11.2.jar").toURI()).toFile());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void writerShouldUseBatching() {
        JdbcSinkSettings settings = new JdbcSinkSettings("jdbc:sqlite:sample.db",
                "tableA",
                new PayloadFields(true, new HashMap<String, String>()),
                true,
                ErrorPolicyEnum.NOOP);
        JdbcDbWriter writer = JdbcDbWriter.from(settings);

        assertEquals(writer.getStatementBuilder().getClass(), BatchedPreparedStatementBuilder.class);
    }

    @Test
    public void writerShouldUseNonBatching() {

        JdbcSinkSettings settings = new JdbcSinkSettings("jdbc:sqlite:sample.db",
                "tableA",
                new PayloadFields(true, new HashMap<String, String>()),
                false,
                ErrorPolicyEnum.NOOP);
        JdbcDbWriter writer = JdbcDbWriter.from(settings);

        assertEquals(writer.getStatementBuilder().getClass(), SinglePreparedStatementBuilder.class);
    }


    @Test
    public void writerShouldUseNoopForErrorHandling() {

        JdbcSinkSettings settings = new JdbcSinkSettings("jdbc:sqlite:sample.db",
                "tableA",
                new PayloadFields(true, Maps.<String, String>newHashMap()),
                true,
                ErrorPolicyEnum.NOOP);
        JdbcDbWriter writer = JdbcDbWriter.from(settings);

        assertEquals(writer.getErrorHandlingPolicy().getClass(), NoopErrorHandlingPolicy.class);
    }

    @Test
    public void writerShouldUseThrowForErrorHandling() {

        JdbcSinkSettings settings = new JdbcSinkSettings("jdbc:sqlite:sample.db", "tableA",
                new PayloadFields(true, new HashMap<String, String>()),
                true,
                ErrorPolicyEnum.THROW);
        JdbcDbWriter writer = JdbcDbWriter.from(settings);

        assertEquals(writer.getErrorHandlingPolicy().getClass(), ThrowErrorHandlingPolicy.class);
    }
}
