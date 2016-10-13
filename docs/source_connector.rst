JDBC Source Connector
=====================

The JDBC source connector allows you to import data from any relational database with a
JDBC driver into Kafka topics. By using JDBC, this connector can support a wide variety of
databases without requiring custom code for each one.

Data is loaded by periodically executing a SQL query and creating an output record for each row
in the result set. By default, all tables in a database are copied, each to its own output topic.
The database is monitored for new or deleted tables and adapts automatically. When copying data
from a table, the connector can load only new or modified rows by specifying which columns should
be used to detect new or modified data.

Quickstart
----------

To see the basic functionality of the connector, we'll copy a single table from a local SQLite
database. In this simple example, we'll assume each entry in the table is assigned a unique ID
and is not modified after creation.

.. note:: You can use your favorite database instead of SQLite.
   Follow the same steps, but adjust the ``connection.url`` setting for your database.
   Confluent Platform includes JDBC drivers for SQLite and PostgreSQL, but if
   you're using a different database you'll also need to make sure the JDBC driver is available on
   the Kafka Connect process's ``CLASSPATH``.

Start by creating a database (you'll need to install SQLite if you haven't already)::

   $ sqlite3 test.db
   SQLite version 3.8.10.2 2015-05-20 18:17:19
   Enter ".help" for usage hints.
   sqlite>

Next in the SQLite command prompt, create a table and seed it with some data::

   sqlite> CREATE TABLE accounts(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, name VARCHAR(255));
   sqlite> INSERT INTO accounts(name) VALUES('alice');
   sqlite> INSERT INTO accounts(name) VALUES('bob');

Now we create a configuration file that will load data from this database. This file is included
with the connector in ``./etc/kafka-connect-jdbc/source-quickstart-sqlite.properties`` and contains the
following settings::

   name=test-sqlite-jdbc-autoincrement
   connector.class=io.confluent.connect.jdbc.JdbcSourceConnector
   tasks.max=1
   connection.url=jdbc:sqlite:test.db
   mode=incrementing
   incrementing.column.name=id
   topic.prefix=test-sqlite-jdbc-

The first few settings are common settings you'll specify for all connectors. The ``connection.url``
specifies the database to connect to, in this case a local SQLite database file. The ``mode``
indicates how we want to query the data. In this case, we have an auto-incrementing unique
ID, so we select ``incrementing`` mode and set ``incrementing.column.name``. In this mode,
each query for new data will only return rows with IDs larger than the maximum ID seen in
previous queries. Finally, we can control the names of the topics
that each table's output is sent to with the ``topic.prefix`` setting. Since we only have one
table, the only output topic in this example will be ``test-sqlite-jdbc-accounts``.

Now, run the connector in a standalone Kafka Connect worker in another terminal (this assumes
Avro settings and that Kafka and the Schema Registry are running locally on the default ports)::

   $ ./bin/connect-standalone ./etc/schema-registry/connect-avro-standalone.properties ./etc/kafka-connect-jdbc/source-quickstart-sqlite.properties

You should see the process start up and log some messages, and then it will begin executing
queries and sending the results to Kafka. In order to check that it has copied the data that was
present when we started Kafka Connect, start a console consumer, reading from the beginning of
the topic::

   $ ./bin/kafka-avro-console-consumer --new-consumer --bootstrap-server localhost:9092 --topic test-sqlite-jdbc-accounts --from-beginning
   {"id":1,"name":{"string":"alice"}}
   {"id":2,"name":{"string":"bob"}}

The output shows the two records as expected, one per line, in the JSON encoding of the Avro
records. Each row is represented as an Avro record and each column is a field in the record. We
can see both columns in the table, ``id`` and ``name``. The IDs were auto-generated and the column
is of type ``INTEGER NOT NULL``, which can be encoded directly as an integer. The ``name`` column
has type ``STRING`` and can be ``NULL``. The JSON encoding of Avro encodes the strings in the
format ``{"type": value}``, so we can see that both rows have ``string`` values with the names
specified when we inserted the data.

Now, keeping the consumer process running, add another record via the SQLite command prompt::

   sqlite> INSERT INTO accounts(name) VALUES('cathy');

If you switch back to the console consumer, you should see the new record added (and,
importantly, the old entries are not repeated)::

   {"id":3,"name":{"string":"cathy"}}

Note that the default polling interval is 5 seconds, so it may take a few seconds to show up.
Depending on your expected rate up updates or desired latency, a smaller poll interval could be
used to deliver updates more quickly.

Of course, :ref:`all the features of Kafka Connect<connect_userguide>`, including offset
management and fault
tolerance, work with the source connector. You can restart and kill the processes and they will
pick up where they left off, copying only new data (as defined by the ``mode`` setting).

Features
--------

The source connector supports copying tables with a variety of JDBC data types, adding and removing
tables from the database dynamically, whitelists and blacklists, varying polling intervals, and
other settings. However, the most important features for most users are the settings controlling
how data is incrementally copied from the database.

Kafka Connect tracks the latest record it retrieved from each table, so it can start in the correct
location on the next iteration (or in case of a crash). The source connector uses this
functionality to only get updated rows from a table (or from the output of a custom query) on each
iteration. Several modes are supported, each of which differs in how modified rows are detected.

Incremental Query Modes
~~~~~~~~~~~~~~~~~~~~~~~

Each incremental query mode tracks a set of columns for each row, which it uses to keep track of
which rows have been processed and which rows are new or have been updated. The ``mode`` setting
controls this behavior and supports the following options:

* **Incrementing Column**: A single column containing a unique ID for each row, where newer rows are
  guaranteed to have larger IDs, i.e. an ``AUTOINCREMENT`` column. Note that this mode can only
  detect *new* rows. *Updates* to existing rows cannot be detected, so this mode should only be
  used for immutable data. One example where you might use this mode is when streaming fact
  tables in a data warehouse, since those are typically insert-only.

* **Timestamp Column**: In this mode, a single column containing a modification timestamp is used
  to track the last time data was processed and to query only for rows that have been modified
  since that time. Note that because timestamps are no necessarily unique, this mode cannot
  guarantee all updated data will be delivered: if 2 rows share the same timestamp and are
  returned by an incremental query, but only one has been processed before a crash, the second
  update will be missed when the system recovers.

* **Timestamp and Incrementing Columns**: This is the most robust and accurate mode, combining an
  incrementing column with a timestamp column. By combining the two, as long as the timestamp is
  sufficiently granular, each (id, timestamp) tuple will uniquely identify an update to a row. Even
  if an update fails after partially completing, unprocessed updates will still be correctly
  detected and delivered when the system recovers.

* **Custom Query**: The source connector supports using custom queries instead of copying whole
  tables. With a custom query, one of the other update automatic update modes can be used as long
  as the necessary ``WHERE`` clause can be correctly appended to the query. Alternatively, the
  specified query may handle filtering to new updates itself;
  however, note that no offset tracking will be performed (unlike the automatic modes where
  ``incrementing`` and/or ``timestamp`` column values are recorded for each record), so the query
  must track offsets itself.

* **Bulk**: This mode is unfiltered and therefore not incremental at all. It will load all rows
  from a table on each iteration. This can be useful if you want to periodically dump an entire
  table where entries are eventually deleted and the downstream system can safely handle duplicates.

Note that all incremental query modes that use certain columns to detect changes will require
indexes on those columns to efficiently perform the queries.

For incremental query modes that use timestamps, the source connector uses a configuration
``timestamp.delay.interval.ms`` to control the waiting period after a row with certain timestamp appears
before we include it in the result. The additional wait allows transactions with earlier timestamps
to complete and the related changes to be included in the result.

Configuration
-------------

The source connector gives you quite a bit of flexibility in the databases you can import data from
and how that data is imported. This section first describes how to access databases whose drivers
are not included with Confluent Platform, then gives a few example configuration files that cover
common scenarios, then provides an exhaustive description of the available configuration options.

JDBC Drivers
~~~~~~~~~~~~

The source connector implements the data copying functionality on the generic JDBC APIs, but relies
on JDBC drivers to handle the database-specific implementation of those APIs. Confluent Platform
ships with a few JDBC drivers, but if the driver for your database is not included you will need
to make it available via the ``CLASSPATH``.

One option is to install the JDBC driver jar alongside the connector. The packaged connector is
installed in the ``share/java/kafka-connect-jdbc`` directory, relative to the installation
directory. If you have installed from Debian or RPM packages, the connector will be installed in
``/usr/share/java/kafka-connect-jdbc``. If you installed from zip or tar files, the connector will
be installed in the path given above under the directory where you unzipped the Confluent
Platform archive.

Alternatively, you can set the ``CLASSPATH`` variable before running ``connect-standalone`` or
``connect-distributed``. For example::

   $ CLASSPATH=/usr/local/firebird/* ./bin/connect-distributed ./config/connect-distributed.properties

would add the JDBC driver for the Firebird database, located in ``/usr/local/firebird``, and allow
you to use JDBC connection URLs like
``jdbc:firebirdsql:localhost/3050:/var/lib/firebird/example.db``.

Examples
~~~~~~~~

The full set of configuration options are listed in the next section, but here we provide a few
template configurations that cover some common usage scenarios.

Use a whitelist to limit changes to a subset of tables in a MySQL database, using ``id`` and
``modified`` columns that are standard on all whitelisted tables to detect rows that have been
modified. This mode is the most robust because it can combine the unique, immutable row IDs with
modification timestamps to guarantee modifications are not missed even if the process dies in the
middle of an incremental update query. ::

   name=mysql-whitelist-timestamp-source
   connector.class=io.confluent.connect.jdbc.JdbcSourceConnector
   tasks.max=10

   connection.url=jdbc:mysql://mysql.example.com:3306/my_database?user=alice&password=secret
   table.whitelist=users,products,transactions

   mode=timestamp+incrementing
   timestamp.column.name=modified
   incrementing.column.name=id

   topic.prefix=mysql-

Use a custom query instead of loading tables, allowing you to join data from multiple tables. As
long as the query does not include its own filtering, you can still use the built-in modes for
incremental queries (in this case, using a timestamp column). Note that this limits you to a single
output per connector and because there is no table name, the topic "prefix" is actually the full
topic name in this case. ::

   name=mysql-whitelist-timestamp-source
   connector.class=io.confluent.connect.jdbc.JdbcSourceConnector
   tasks.max=10

   connection.url=jdbc:postgresql://postgres.example.com/test_db?user=bob&password=secret&ssl=true
   query=SELECT users.id, users.name, transactions.timestamp, transactions.user_id, transactions.payment FROM users JOIN transactions ON (users.id = transactions.user_id)
   mode=timestamp
   timestamp.column.name=timestamp

   topic.prefix=mysql-joined-data

Schema Evolution
----------------

The JDBC connector supports schema evolution when the Avro converter is used. When there is a
change in a database table schema, the JDBC connector can detect the change, create a new Kafka
Connect schema and try to register a new Avro schema in the Schema Registry. Whether we can
successfully register the schema or not depends on the compatibility level of the Schema Registry,
which is backward by default.

For example, if we remove a column from a table, the change is backward compatible and the
corresponding Avro schema can be successfully registered in the Schema Registry. If we modify
the database table schema to change a column type or add a column, when the Avro schema is
registered to the Schema Registry, it will be rejected as the changes are not backward compatible.

You can change the compatibility level of Schema Registry to allow incompatible schemas or other
compatibility levels. There are two ways to do this:

* Set the compatibility level for subjects which are used by the connector using
  ``PUT /config/(string: subject)``. The subjects have format of ``topic-key`` and ``topic-value``
  where the ``topic`` is determined by ``topic.prefix`` config and table name.

* Configure the Schema Registry to use other schema compatibility level by setting
  ``avro.compatibility.level`` in Schema Registry. Note that this is a global setting that applies
  to all schemas in the Schema Registry.

However, due to the limitation of the JDBC API, some compatible schema changes may be treated as
incompatible change. For example, adding a column with default value is a backward compatible
change. However, limitations of the JDBC API make it difficult to map this to default
values of the correct type in a Kafka Connect schema, so the default values are currently omitted.
The implications is that even some changes of the database table schema is backward compatible, the
schema registered in the Schema Registry is not backward compatible as it doesn't contain a default
value.

If the JDBC connector is used together with the HDFS connector, there are some restrictions to schema
compatibility as well. When Hive integration is enabled, schema compatibility is required to be
backward, forward and full to ensure that the Hive schema is able to query the whole data under a
topic. As some compatible schema change will be treated as incompatible schema change, those
changes will not work as the resulting Hive schema will not be able to query the whole data for a
topic.
