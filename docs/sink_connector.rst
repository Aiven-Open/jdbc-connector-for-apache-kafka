JDBC Sink Connector
===================

The JDBC sink connector allows you to export data from Kafka topics to any relational database with a JDBC driver.
By using JDBC, this connector can support a wide variety of databases without requiring a dedicated connector for each one.
The connector polls data from Kafka to write to the database based on the topics subscription.
It is possible to achieve idempotent writes with upserts.
Auto-creation of tables, and limited auto-evolution is also supported.

Quick Start
-----------

.. include:: includes/prerequisites.rst
        :start-line: 2
        :end-line: 8

To see the basic functionality of the connector, we'll be copying Avro data from a single topic to a local SQLite database.

.. include:: includes/prerequisites.rst
    :start-line: 11
    :end-line: 45

----------------------------
Load the JDBC Sink Connector
----------------------------

Load the predefined JDBC sink connector.

#.  Optional: View the available predefined connectors with this command:

    .. sourcecode:: bash

        confluent list connectors

    Your output should resemble:

    .. sourcecode:: bash

        Bundled Predefined Connectors (edit configuration under etc/):
          elasticsearch-sink
          file-source
          file-sink
          jdbc-source
          jdbc-sink
          hdfs-sink
          s3-sink

#.  Load the the ``jdbc-sink`` connector:

    .. sourcecode:: bash

        confluent load jdbc-sink

    Your output should resemble:

    .. sourcecode:: bash

        {
          "name": "jdbc-sink",
          "config": {
            "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
            "tasks.max": "1",
            "topics": "orders",
            "connection.url": "jdbc:sqlite:test.db",
            "auto.create": "true",
            "name": "jdbc-sink"
          },
          "tasks": [],
          "type": null
        }

.. tip:: For non-CLI users, you can load the JDBC sink connector with this command:

    .. sourcecode:: bash

        <path-to-confluent>/bin/connect-standalone \
        <path-to-confluent>/etc/schema-registry/connect-avro-standalone.properties \
        <path-to-confluent>/etc/kafka-connect-jdbc/sink-quickstart-sqlite.properties

--------------------------
Produce a Record in SQLite
--------------------------

#.  Produce a record into the ``orders`` topic.

    .. sourcecode:: bash

        $ ./bin/kafka-avro-console-producer \
         --broker-list localhost:9092 --topic orders \
         --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"id","type":"int"},{"name":"product", "type": "string"}, {"name":"quantity", "type": "int"}, {"name":"price",
         "type": "float"}]}'

    The console producer waits for input.

#.  Copy and paste the following record into the terminal and press **Enter**:

    .. sourcecode:: bash

        {"id": 999, "product": "foo", "quantity": 100, "price": 50}

#.  Query the SQLite database and you should see that the ``orders`` table was automatically created and contains the record.

    .. sourcecode:: bash

        $ sqlite3 test.db
        sqlite> SELECT * from orders;
        foo|50.0|100|999

Features
--------

Data mapping
^^^^^^^^^^^^

The sink connector requires knowledge of schemas, so you should use a suitable converter e.g. the Avro converter that comes with the schema registry, or the JSON converter with schemas enabled.
Kafka record keys if present can be primitive types or a Connect struct, and the record value must be a Connect struct.
Fields being selected from Connect structs must be of primitive types.
If the data in the topic is not of a compatible format, implementing a custom ``Converter`` may be necessary.

Key handling
^^^^^^^^^^^^

The default is for primary keys to not be extracted with ``pk.mode`` set to `none`,
which is not suitable for advanced usage such as upsert semantics and when the connector is responsible for auto-creating the destination table.
There are different modes that enable to use fields from the Kafka record key, the Kafka record value, or the Kafka coordinates for the record.

Refer to :ref:`primary key configuration options <sink-pk-config-options>` for further detail.

Idempotent writes
^^^^^^^^^^^^^^^^^

The default ``insert.mode`` is `insert`. If it is configured as `upsert`, the connector will use upsert semantics rather than plain `INSERT` statements.
Upsert semantics refer to atomically adding a new row or updating the existing row if there is a primary key constraint violation, which provides idempotence.

If there are failures, the Kafka offset used for recovery may not be up-to-date with what was committed as of the time of the failure, which can lead to re-processing during recovery.
The upsert mode is highly recommended as it helps avoid constraint violations or duplicate data if records need to be re-processed.

Aside from failure recovery, the source topic may also naturally contain multiple records over time with the same primary key, making upserts desirable.

As there is no standard syntax for upsert, the following table describes the database-specific DML that is used.

===========     ================================================
Database        Upsert style
===========     ================================================
MySQL           `INSERT .. ON DUPLICATE KEY REPLACE ..`
Oracle          `MERGE ..`
PostgreSQL      `INSERT .. ON CONFLICT .. DO UPDATE SET ..`
SQLite          `INSERT OR REPLACE ..`
SQL Server      `MERGE ..`
Other           *not supported*
===========     ================================================

Auto-creation and Auto-evoluton
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. tip:: Make sure the JDBC user has the appropriate permissions for DDL.

If ``auto.create`` is enabled, the connector can `CREATE` the destination table if it is found to be missing.
The creation takes place online with records being consumed from the topic, since the connector uses the record schema as a basis for the table definition.
Primary keys are specified based on the key configuration settings.

If ``auto.evolve`` is enabled, the connector can perform limited auto-evolution by issuing `ALTER` on the destination table when it encounters a record for which a column is found to be missing.
Since data-type changes and removal of columns can be dangerous, the connector does not attempt to perform such evolutions on the table.
Addition of primary key constraints is also not attempted.

For both auto-creation and auto-evolution, the nullability of a column is based on the optionality of the corresponding field in the schema,
and default values are also specified based on the default value of the corresponding field if applicable.
We use the following mapping from Connect schema types to database-specific types:

+-------------+-----------------+-----------------+------------------+---------+----------------+-----------------+
| Schema Type | MySQL           | Oracle          | PostgreSQL       | SQLite  | SQL Server     | Vertica         |
+=============+=================+=================+==================+=========+================+=================+
| INT8        | TINYINT         | NUMBER(3,0)     | SMALLINT         | NUMERIC | TINYINT        | INT             |
+-------------+-----------------+-----------------+------------------+---------+----------------+-----------------+
| INT16       | SMALLINT        | NUMBER(5,0)     | SMALLINT         | NUMERIC | SMALLINT       | INT             |
+-------------+-----------------+-----------------+------------------+---------+----------------+-----------------+
| INT32       | INT             | NUMBER(10,0)    | INT              | NUMERIC | INT            | INT             |
+-------------+-----------------+-----------------+------------------+---------+----------------+-----------------+
| INT64       | BIGINT          | NUMBER(19,0)    | BIGINT           | NUMERIC | BIGINT         | INT             |
+-------------+-----------------+-----------------+------------------+---------+----------------+-----------------+
| FLOAT32     | FLOAT           | BINARY_FLOAT    | REAL             | REAL    | REAL           | FLOAT           |
+-------------+-----------------+-----------------+------------------+---------+----------------+-----------------+
| FLOAT64     | DOUBLE          | BINARY_DOUBLE   | DOUBLE PRECISION | REAL    | FLOAT          | FLOAT           |
+-------------+-----------------+-----------------+------------------+---------+----------------+-----------------+
| BOOLEAN     | TINYINT         | NUMBER(1,0)     | BOOLEAN          | NUMERIC | BIT            | BOOLEAN         |
+-------------+-----------------+-----------------+------------------+---------+----------------+-----------------+
| STRING      | VARCHAR(256)    | NCLOB           | TEXT             | TEXT    | VARCHAR(MAX)   | VARCHAR(1024)   |
+-------------+-----------------+-----------------+------------------+---------+----------------+-----------------+
| BYTES       | VARBINARY(1024) | BLOB            | BYTEA            | BLOB    | VARBINARY(MAX) | VARBINARY(1024) |
+-------------+-----------------+-----------------+------------------+---------+----------------+-----------------+
| 'Decimal'   | DECIMAL(65,s)   | NUMBER(*,s)     | DECIMAL          | NUMERIC | DECIMAL(38,s)  | DECIMAL(18,s)   |
+-------------+-----------------+-----------------+------------------+---------+----------------+-----------------+
| 'Date'      | DATE            | DATE            | DATE             | NUMERIC | DATE           | DATE            |
+-------------+-----------------+-----------------+------------------+---------+----------------+-----------------+
| 'Time'      | TIME(3)         | DATE            | TIME             | NUMERIC | TIME           | TIME            |
+-------------+-----------------+-----------------+------------------+---------+----------------+-----------------+
| 'Timestamp' | TIMESTAMP(3)    | TIMESTAMP       | TIMESTAMP        | NUMERIC | DATETIME2      | TIMESTAMP       |
+-------------+-----------------+-----------------+------------------+---------+----------------+-----------------+

Auto-creation or auto-evolution is not supported for databases not mentioned here.

.. important::
    For backwards-compatible table schema evolution, new fields in record schemas must be optional or have a default value.
    If you need to delete a field, the table schema should be manually altered to either drop the corresponding column, assign it a default value, or make it nullable.
