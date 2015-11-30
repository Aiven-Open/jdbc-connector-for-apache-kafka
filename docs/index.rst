.. Kafka Connect JDBC documentation master file

JDBC Connector
==============

The JDBC connector allows you to import data from any relational database with a
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
and is not modified after creation. Start by creating a database (you'll need to install SQLite
if you haven't already)::

   $ sqlite3 test.db
   SQLite version 3.8.10.2 2015-05-20 18:17:19
   Enter ".help" for usage hints.
   sqlite>

Next in the SQLite command prompt, create a table and seed it with some data::

   sqlite> CREATE TABLE accounts(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, name VARCHAR(255) NOT NULL);
   sqlite> INSERT INTO accounts(name) VALUES('alice');
   sqlite> INSERT INTO accounts(name) VALUES('bob');

Now, create a configuration file that will load data from this database in
``quickstart-sqlite.properties``::

   name=test-sqlite-jdbc-autoincrement
   connector.class=io.confluent.connect.jdbc.JdbcSourceConnector
   tasks.max=1
   connection.url=jdbc:sqlite:test.db
   mode=increasing
   increasing.column.name=id
   topic.prefix=test-sqlite-jdbc-

The first few settings are common settings you'll specify for all connectors. The ``connection.url``
specifies the database to connect to, in this case a local SQLite database file. The ``mode``
indicates how we want to query the data. In this case, we have an auto-incrementing unique
ID, so we select ``increasing`` mode where each query uses the largest ID previously seen to limit
new results to newly created rows. The ``increasing.column.name`` setting specifies the column in
the table that contains the auto-incrementing ID. Finally, we can control the names of the topics
that each table's output is sent to with the ``topic.prefix`` setting. Since we only have one
table, the only output topic in this example will be ``test-sqlite-jdbc-accounts``.

Now, run the connector in a standalone Kafka Connect worker in another terminal (this assumes
Avro settings and that Kafka and the Schema Registry are running locally on the default ports)::

   $ ./bin/connect-standalone ./config/connect-standalone.properties quickstart-sqlite.properties

You should see the process start up and log some messages, and then it will begin executing
queries and sending the results to Kafka. In order to check that it has copied the data that was
present when we started Kafka Connect, start a console consumer, reading from the beginning of
the topic::

   $ ./bin/kafka-avro-console-consumer --new-consumer --bootstrap-server localhost:9092 --topic test-sqlite-jdbc-accounts --from-beginning
   {"id":1,"name":{"string":"alice"}}
   {"id":2,"name":{"string":"bob"}}

The output shows the two records as expected, in the JSON encoding of the Avro records. Now,
keeping that running, add another record via the SQLite command prompt::

   sqlite> INSERT INTO accounts(name) VALUES('cathy');

If you switch back to the console consumer, you should see the new record added (and,
importantly, the old entries are not repeated)::

   {"id":3,"name":{"string":"cathy"}}

Note that the default polling interval is 5 seconds, so it may take a few seconds to show up.
Depending on your expected rate up updates or desired latency, a smaller poll interval could be
used to deliver updates more quickly.

Of course, all the normal features of Kafka Connect, including offset management and fault
tolerance, work with the JDBC connector. You can restart and kill the processes and they will
pick up where they left off, copying only new data (as defined by the ``mode`` setting).

Features
--------

The JDBC connector supports copying tables with a variety of JDBC data types, adding and removing
tables from the database dynamically, whitelists and blacklists, varying polling intervals, and
other settings. However, the most important features for most users are the settings controlling
how data is incrementally copied from the database.

Mapping to Kafka Connect Partitioned Streams & Offsets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Kafka Connect uses `partitioned streams` as its core data model. In order to map to Kafka Connect
records, the JDBC connector must model its data as a set of partitions, each with an ordered
sequence of updates with unique offsets. In the case of a process restart or failure, these
offsets are used to resume reading from the last position that the system can guarantee has been
processed.

To map to this model, the JDBC connector must map partitions and offsets. Partitions is
handled on a per-table basis: each table represents a single partition and every insertion or
update of a row corresponds to a single record in that partition. As a special case, custom
queries (which disable table-based processing) are treated as a single-partition stream.

Offsets now must be defined for each table's stream. The JDBC connector supports multiple options
depending on the format of data within your tables. However, at their core, all settings share a
common approach. A column or set of columns is used to define and order offsets. A simple example
uses only unique, auto-incrementing IDs in a table where rows are only inserted, never updated.
In this mode, offsets are a single integer ID and are naturally ordered. Querying for
only new data is trivially handled by checking for IDs with larger values than the
largest seen so far. "Rewinding" upon failure simply requires looking up the largest ID
previously seen and starting queries with that value.

However, many tables cannot be guaranteed to only have insertions and have a unique,
auto-incrementing ID. In order to support a variety of tables with different properties, multiple
different approaches to defining offsets & queries for new or updated rows are supported.

Incremental Query Modes
~~~~~~~~~~~~~~~~~~~~~~~

* **Increasing Column**: In this mode, a single column is specified and contains a unique ID for
  each row which increases with each insertion. Although it could be implemented differently, this
  type of column is most frequently implemented as an ``AUTOINCREMENT`` column in a table where rows
  are only inserted, but not updated. (Note that updates will *not* be handled properly in this
  mode because they cannot be detected once the row has been processed for the first time.)

* **Timestamp Column**: In this mode, a single column is specified and contains a modification
  timestamp for each row. Using a timestamp makes it simple to query for updated data by simply
  requesting any rows with newer timestamps than the most recent timestamp previously observed.
  However, these timestamps are also used as offsets and therefore technically violate the
  uniqueness requirement for offsets. Because of this, this mode cannot guarantee all updated data
  will be delivered: if 2 rows share the same offset and are returned by an incremental query, but
  only one has been processed before a crash, the second update will be missed when the system
  recovers.

* **Timestamp and Increasing Columns**: This is the most robust and accurate mode, combining an
  increasing column with a timestamp column. By combining the two, as long as the timestamp is
  sufficiently granular, each (id, timestamp) tuple will uniquely identify a row update. Even if an
  update fails midway, when the system recovers, unprocessed updates will still be correctly
  detected and delivered.

* **Custom Query**: The JDBC connector supports using custom queries instead of copying whole
  tables. With a custom query, one of the other update automatic update modes can be used as long
  as the necessary ``WHERE`` clause can be correctly appended (set the appropriate ``mode``
  configuration). Alternatively, the specified query may handle filtering to new updates itself;
  however, note that no offset tracking will be performed (unlike the automatic modes where
  ``increasing`` and/or ``timestamp`` column values are recorded for each record), so the query
  must track offsets itself.

* **Bulk**: This mode is unfiltered and therefore not incremental at all. It will load all rows
  from a table on each iteration. This can be useful if you want to periodically dump an entire
  table where entries are eventually deleted and the downstream system can safely handle duplicates.

Configuration
-------------

The JDBC connector gives you quite a bit of flexibility in the databases you can import data from
and how that data is imported. This section first describes how to access databases whose drivers
are not included with Confluent Platform, then gives a few example configuration files that cover
common scenarios, then provides an exhaustive description of the available configuration options.

JDBC Drivers
~~~~~~~~~~~~

The JDBC connector implements the data copying functionality on the generic JDBC APIs, but relies
on JDBC drivers to handle the database-specific implementation of those APIs. Confluent Platform
ships with a few JDBC drivers, but if the driver for your database is not included you will need
to make it available via the ``CLASSPATH``.

One option is to install the JDBC driver jar alongside the connector. The packaged connector is
installed in the ``share/java/kafka-connect-jdbc`` directory, relative to the installation
directory. If you have installed from Debian or RPM packages, the connector will be installed in
``/usr/share/java/kafka-connect-jdbc``. If you installed from zip or tar files, the connector will
be installed in the path given above under the directory where you unzipped the Confluent
Platform archive.

Alternatively, you can set the ``CLASSPATH`` variable before running ``copycat-standalone`` or
``copycat-distributed``. For example,::

   $ CLASSPATH=/usr/local/firebird/* ./bin/copycat-distributed ./config/copycat-distributed.properties

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

   mode=timestamp+increasing
   timestamp.column.name=modified
   increasing.column.name=id

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

Configuration Options
~~~~~~~~~~~~~~~~~~~~~

``connection.url``
  JDBC connection URL for the database to load.

  * Type: string
  * Default: ""
  * Importance: high

``topic.prefix``
  Prefix to prepend to table names to generate the name of the Kafka topic to publish data to, or in the case of a custom query, the full name of the topic to publish to.

  * Type: string
  * Default: ""
  * Importance: high

``mode``
  The mode for updating a table each time it is polled. Options include:

    * bulk - perform a bulk load of the entire table each time it is polled

    * increasing - use a strictly increasing column on each table to detect only new rows. Note that this will not detect modifications or deletions of existing rows.

    * timestamp - use a timestamp (or timestamp-like) column to detect new and modified rows. This assumes the column is updated with each write, and that values are monotonically increasing, but not necessarily unique.

    * timestamp+increasing - use two columns, a timestamp column that detects new and modified rows and a strictly increasing column which provides a globally unique ID for updates so each row can be assigned a unique stream offset.

  * Type: string
  * Default: ""
  * Importance: high

``poll.interval.ms``
  Frequency in ms to poll for new data in each table.

  * Type: int
  * Default: 5000
  * Importance: high

``increasing.column.name``
  The name of the strictly increasing column to use to detect new rows. Any empty value indicates the column should be autodetected by looking for an auto-incrementing column. This column may not be nullable.

  * Type: string
  * Default: ""
  * Importance: medium

``query``
  If specified, the query to perform to select new or updated rows. Use this setting if you want to join tables, select subsets of columns in a table, or filter data. If used, this connector will only copy data using this query -- whole-table copying will be disabled. Different query modes may still be used for incremental updates, but in order to properly construct the incremental query, it must be possible to append a WHERE clause to this query (i.e. no WHERE clauses may be used). If you use a WHERE clause, it must handle incremental queries itself.

  * Type: string
  * Default: ""
  * Importance: medium

``table.blacklist``
  List of tables to exclude from copying. If specified, table.whitelist may not be set.

  * Type: list
  * Default: []
  * Importance: medium

``table.whitelist``
  List of tables to include in copying. If specified, table.blacklist may not be set.

  * Type: list
  * Default: []
  * Importance: medium

``timestamp.column.name``
  The name of the timestamp column to use to detect new or modified rows. This column may not be nullable.

  * Type: string
  * Default: ""
  * Importance: medium

``batch.max.rows``
  Maximum number of rows to include in a single batch when polling for new data. This setting can be used to limit the amount of data buffered internally in the connector.

  * Type: int
  * Default: 100
  * Importance: low

``table.poll.interval.ms``
  Frequency in ms to poll for new or removed tables, which may result in updated task configurations to start polling for data in added tables or stop polling for data in removed tables.

  * Type: long
  * Default: 60000
  * Importance: low
