package io.aiven.connect.jdbc.sink;

import io.aiven.connect.jdbc.dialect.DatabaseDialect;
import io.aiven.connect.jdbc.util.TableId;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class BufferManager {
    private final JdbcSinkConfig config;
    private final DatabaseDialect dbDialect;
    private final DbStructure dbStructure;
    private final Connection connection;
    private final Map<TableId, BufferedRecords> bufferByTable = new HashMap<>();

    public BufferManager(JdbcSinkConfig config, DatabaseDialect dbDialect, DbStructure dbStructure, Connection connection) {
        this.config = config;
        this.dbDialect = dbDialect;
        this.dbStructure = dbStructure;
        this.connection = connection;
    }

    public void addRecord(SinkRecord record) throws SQLException {
        final TableId tableId = destinationTable(record.topic());
        BufferedRecords buffer = bufferByTable.get(tableId);
        if (buffer == null) {
            buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, connection);
            bufferByTable.put(tableId, buffer);
        }
        buffer.add(record);
    }

    public void flushAndClose() throws SQLException {
        for (Map.Entry<TableId, BufferedRecords> entry : bufferByTable.entrySet()) {
            final BufferedRecords buffer = entry.getValue();
            buffer.flush();
            buffer.close();
        }
        bufferByTable.clear();
    }

    private TableId destinationTable(final String topic) {
        return dbDialect.parseTableIdentifier(TableNameGenerator.generateTableName(config, topic));
    }
}
