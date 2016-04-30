package com.datamountaineer.streamreactor.connect;


import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.connect.avro.AvroData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;

import java.util.HashMap;

/**
 * Created by andrew@datamountaineer.com on 22/02/16.
 * stream-reactor
 */

public final class ConverterUtil {
    private final static JsonConverter jsonConverter = new JsonConverter();
    private final static JsonDeserializer deserializer = new JsonDeserializer();
    private final static AvroData avroData = new AvroData(100);

    /**
     * Convert a SinkRecords value to a Json string using Kafka Connects deserializer
     *
     * @param record A SinkRecord to extract the payload value from
     * @return A json string for the payload of the record
     */
    public static JsonNode convertValueToJson(final SinkRecord record) {
        final byte[] converted = jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        return deserializeToJson(record.topic(), converted);
    }

    /**
     * Convert a SinkRecords key to a Json string using Kafka Connects deserializer
     *
     * @param record A SinkRecord to extract the payload value from
     * @return A json string for the payload of the record
     */
    public static JsonNode convertKeyToJson(final SinkRecord record) {
        final byte[] converted = jsonConverter.fromConnectData(record.topic(), record.keySchema(), record.key());
        return deserializeToJson(record.topic(), converted);
    }

    /**
     * Deserialize Byte array for a topic to json
     *
     * @param topic   Topic name for the byte array
     * @param payload Byte Array payload
     * @return A JsonNode representing the byte array
     */
    public static JsonNode deserializeToJson(final String topic, final byte[] payload) {
        JsonNode json = deserializer.deserialize(topic, payload).get("payload");
        //logger.debug(s"Converted to $json.")
        return json;
    }

    /**
     * Configure the converter
     *
     * @param converter The Converter to configure
     * @param props     The props to configure with
     */
    public final void configureConverter(final Converter converter, final HashMap<String, String> props) {
        converter.configure(props, false);
    }

    /**
     * Convert SinkRecord to GenericRecord
     *
     * @param record SinkRecord to convert
     * @return a GenericRecord
     **/
    public static GenericRecord convertToGenericAvro(final SinkRecord record) {
        return (GenericRecord) avroData.fromConnectData(record.valueSchema(), record.value());
    }
}

