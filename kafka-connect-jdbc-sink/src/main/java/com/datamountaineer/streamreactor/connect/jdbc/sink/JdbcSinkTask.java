
/**
 * Copyright 2015 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.datamountaineer.streamreactor.connect.jdbc.sink;

import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkSettings;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.JdbcDbWriter;
import com.google.common.io.CharStreams;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Map;

/**
 * <h1>JdbcSinkTask</h1>
 * <p>
 * Kafka Connect Jdbc sink task. Called by framework to put records to the
 * target sink
 **/
class JdbcSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(JdbcSinkTask.class);
    private JdbcDbWriter writer = null;

    /**
     * Parse the configurations and setup the writer
     *
     * @param props A Map of properties to set the tasks
     **/
    @Override
    public void start(final Map<String, String> props) {
        try {
            final String ascii = CharStreams.toString(new InputStreamReader(getClass().getResourceAsStream("/jdbc.ascii")));
            logger.info(ascii);
        } catch (IOException e) {
            logger.warn("Can't load the ascii art!");
        }

        //Parse and check the configuration
        JdbcSinkConfig.config.parse(props);
        final JdbcSinkConfig sinkConfig = new JdbcSinkConfig(props);
        final JdbcSinkSettings settings = JdbcSinkSettings.from(sinkConfig);
        logger.info(String.format("Settings:" + settings.toString()));

        //Set up the writer
        writer = JdbcDbWriter.from(settings);
    }

    /**
     * Pass the SinkRecords to the writer for Writing
     *
     * @param records The sink records to write to the database
     **/
    @Override
    public void put(Collection<SinkRecord> records)
    {
        if (records.isEmpty()) {
            logger.info("Empty list of records received.");
        }
        else {
            assert (writer != null) : "Writer is not set!";
            writer.write(records);
        }
    }

    /**
     * Clean up Jdbc connections
     **/
    @Override
    public void stop() {
        logger.info("Stopping Jdbc sink.");
        writer.close();
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        //TODO
        //have the writer expose a is busy; can expose an await using a countdownlatch internally
    }

    @Override
    public String version() {
        return getClass().getPackage().getImplementationVersion();

    }

}