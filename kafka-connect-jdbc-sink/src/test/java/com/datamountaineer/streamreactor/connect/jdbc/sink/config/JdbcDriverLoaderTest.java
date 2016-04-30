package com.datamountaineer.streamreactor.connect.jdbc.sink.config;

import com.datamountaineer.streamreactor.connect.jdbc.sink.JdbcDriverLoader;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class JdbcDriverLoaderTest {
    @Test(expected = IllegalArgumentException.class)
    public void throwAnExceptionIfTheJdbcJarDoesNotExists() {
        JdbcDriverLoader.load("somedriver", new File("bogus.jar"));
    }

    @Test
    public void loadTheOracleDriver() throws URISyntaxException {
        File jar = Paths.get(getClass().getResource("/ojdbc7.jar").toURI()).toAbsolutePath().toFile();
        String driver = "oracle.jdbc.OracleDriver";
        assertEquals(JdbcDriverLoader.load(driver, jar), true);
        assertEquals(JdbcDriverLoader.load(driver, jar), false);
    }

    @Test
    public void loadTheMSDriver() throws URISyntaxException {
        File jar = Paths.get(getClass().getResource("/sqljdbc4-4-4.0.jar").toURI()).toAbsolutePath().toFile();
        String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        assertEquals(JdbcDriverLoader.load(driver, jar), true);
        assertEquals(JdbcDriverLoader.load(driver, jar), false);
    }
}
