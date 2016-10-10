JDBC Sink Connector
===================

The JDBC sink connector allows you to export data from Kafka topics to any relational database with a JDBC driver.
By using JDBC, this connector can support a wide variety of databases without requiring a dedicated connector for each one.
The connector polls data from Kafka to write to the database based on the topics subscription.
It is possible to achieve idempotent writes with upserts.
Auto-creation of tables, and limited auto-evolution is also supported.

Quickstart
----------

To see the basic functionality of the connector, we'll be copying Avro data from a single topic to a local SQLite database.
This example assumes you are running Kafka and Schema Registry locally on the default ports.

.. note::
    We use SQLite in these examples, but you can use your favorite database.
    Follow the same steps, but adjust the ``connection.url`` setting for your database.
    Confluent Platform includes JDBC drivers for SQLite and PostgreSQL,
    but if you're using a different database you'll also need to make sure the JDBC driver is available on the Kafka Connect process's ``CLASSPATH``.

Let's create a configuration file for the connector.
This file is included with the connector in ``./etc/kafka-connect-jdbc/sink-quickstart-sqlite.properties`` and contains the following settings::

    name=test-sink
    connector.class=io.confluent.connect.jdbc.JdbcSinkConnector
    tasks.max=1
    topics=orders
    connection.url=jdbc:sqlite:test.db
    auto.create=true

The first few settings are common settings you'll specify for all connectors, except for ``topics`` which is specific to sink connectors like this one.
The ``connection.url`` specifies the database to connect to, in this case a local SQLite database file.
Enabling ``auto.create`` allows us to rely on the connector for creating the table.

Now we can run the connector with this configuration.

.. sourcecode:: bash

    $ ./bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties etc/kafka-connect-jdbc/sink-quickstart-sqlite.properties

Now, we will produce a record into the `orders` topic.

.. sourcecode:: bash

    $ bin/kafka-avro-console-producer \
     --broker-list localhost:9092 --topic orders \
     --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"id","type":"int"},{"name":"product", "type": "string"}, {"name":"quantity", "type": "int"}, {"name":"price",
     "type": "float"}]}'

The console producer is waiting for input. Copy and paste the following record into the terminal:

.. sourcecode:: bash

    {"id": 999, "product": "foo", "quantity": 100, "price": 50}

Now if we query the database, we will see that the `orders` table was automatically created and contains the record.

.. sourcecode:: bash

    $ sqlite3 test.db
    sqlite> select * from orders;
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

+-------------+-----------------+-----------------+------------------+---------+----------------+
| Schema Type | MySQL           | Oracle          | PostgreSQL       | SQLite  | SQL Server     |
+=============+=================+=================+==================+=========+================+
| INT8        | TINYINT         | NUMBER(3,0)     | SMALLINT         | NUMERIC | TINYINT        |
+-------------+-----------------+-----------------+------------------+---------+----------------+
| INT16       | SMALLINT        | NUMBER(5,0)     | SMALLINT         | NUMERIC | SMALLINT       |
+-------------+-----------------+-----------------+------------------+---------+----------------+
| INT32       | INT             | NUMBER(10,0)    | INT              | NUMERIC | INT            |
+-------------+-----------------+-----------------+------------------+---------+----------------+
| INT64       | BIGINT          | NUMBER(19,0)    | BIGINT           | NUMERIC | BIGINT         |
+-------------+-----------------+-----------------+------------------+---------+----------------+
| FLOAT32     | FLOAT           | BINARY_FLOAT    | REAL             | REAL    | REAL           |
+-------------+-----------------+-----------------+------------------+---------+----------------+
| FLOAT64     | DOUBLE          | BINARY_DOUBLE   | DOUBLE PRECISION | REAL    | FLOAT          |
+-------------+-----------------+-----------------+------------------+---------+----------------+
| BOOLEAN     | TINYINT         | NUMBER(1,0)     | BOOLEAN          | NUMERIC | BIT            |
+-------------+-----------------+-----------------+------------------+---------+----------------+
| STRING      | VARCHAR(256)    | NCLOB           | TEXT             | TEXT    | VARCHAR(MAX)   |
+-------------+-----------------+-----------------+------------------+---------+----------------+
| BYTES       | VARBINARY(1024) | BLOB            | BYTEA            | BLOB    | VARBINARY(MAX) |
+-------------+-----------------+-----------------+------------------+---------+----------------+
| 'Decimal'   | DECIMAL(65,s)   | NUMBER(*,s)     | DECIMAL          | NUMERIC | DECIMAL(38,s)  |
+-------------+-----------------+-----------------+------------------+---------+----------------+
| 'Date'      | DATE            | DATE            | DATE             | NUMERIC | DATE           |
+-------------+-----------------+-----------------+------------------+---------+----------------+
| 'Time'      | TIME(3)         | DATE            | TIME             | NUMERIC | TIME           |
+-------------+-----------------+-----------------+------------------+---------+----------------+
| 'Timestamp' | TIMESTAMP(3)    | TIMESTAMP       | TIMESTAMP        | NUMERIC | DATETIME2      |
+-------------+-----------------+-----------------+------------------+---------+----------------+

Auto-creation or auto-evolution is not supported for databases not mentioned here.

.. important::
    For backwards-compatible table schema evolution, new fields in record schemas must be optional or have a default value.
    If you need to delete a field, the table schema should be manually altered to either drop the corresponding column, assign it a default value, or make it nullable.
