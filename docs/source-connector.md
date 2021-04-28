# Kafka Connect JDBC Source Connector

This [Kafka Connect](https://kafka.apache.org/documentation/#connect)
connector allows you to transfer data from a relational database into
Apache Kafka topics.

[Full configuration options reference](source-connector-config-options.rst).

## How It Works

The connector works with multiple data sources (tables, views; a custom
query) in the database. For each data source, there is a corresponding
Kafka topic.

The connector connects to the database and periodically queries its data
sources. The poll interval is configured by `poll.interval.ms` and is 5
seconds by default.

Each row from the database response is transformed into a record and
published into the corresponding Kafka topic.

## Database

### Connection

The connector is instructed how to connect to the database using
`connection.url`, `connection.user` and `connection.password`
configurations.

Some database drivers support SSL encryption of the connection, which is
configured with the connection URL as well.

The format of the connection URL is specific to the database driver.
Here are some documentations:
- [for PostgreSQL](https://jdbc.postgresql.org/documentation/head/connect.html);
- [for MySQL](https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-jdbc-url-format.html);
- [for MariaDB](https://mariadb.com/kb/en/library/about-mariadb-connector-j/#connection-strings);
- [for SQLite](http://www.sqlitetutorial.net/sqlite-java/sqlite-jdbc-driver/);
- [for Derby](https://db.apache.org/derby/docs/10.15/devguide/cdevdvlp17453.html).

For example, for PostgreSQL the connection URL might look like
```
jdbc:postgresql://localhost:5432/test?user=fred&password=secret&ssl=true
```

#### Setting Default Fetch Size to Prevent Out-of-Memory Errors

By default, some JDBC drivers (e.g., PostgreSQL's and MySQL's) fetch all
the rows from the database after executing a query and store them in
memory. This is not practical in the case of this Source connector due
to two reasons:
1. A query may result in too many rows, storing which in memory will
   cause out-of-memory errors.
2. The number of rows posted to Kafka on a single poll is limited by
   `batch.max.rows`.

To address this potential issue, some drivers provide a parameter that
is set in URL and defines the default number of rows to fetch and keep
in memory.

[PostgreSQL's driver](https://jdbc.postgresql.org/documentation/head/connect.html)
uses `defaultRowFetchSize`. The URL with it might look like
```
jdbc:postgresql://localhost:5432/test?<other_properties>&defaultRowFetchSize=1000
```

In
[MySQL's driver](https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-configuration-properties.html),
it is called `defaultFetchSize`. Also setting `useCursorFetch=true` is required.
The URL with it might look like
```
jdbc:mysq://localhost:3306/test?<other_properties>&useCursorFetch=true&defaultFetchSize=1000
```

Some drivers require autocommit to be disabled along with setting the
default fetch size to operate in this mode. The connector does this.

Check the driver documentation for the availability of these parameter,
its name and if it is need to be set at all.

### SQL Dialects

Different databases use different dialects of SQL. The connector
supports several of them.

By default, the connector will automatically determine the dialect based
on the connection URL (`connection.url`).

If needed, it's possible to specify the dialect explicitly using
`dialect.name` configuration. Check
[the configuration's reference](source-connector-config-options.rst#database)
for the list of supported dialects.

## Data Sources

The connector can be configured to work with two types of data sources:
- tables (including other database objects like views, synonyms, etc.);
- a custom `SELECT` query.

### Tables

By default, the connector works only with tables, considering all tables
in all catalogs and schemas. This can be changed in several ways:

1. By limiting the table discovery to a particular catalog by setting
   `catalog.pattern` configuration.
2. By limiting the table discovery to some schemas by setting
   `schema.pattern` configuration.
3. By limiting the table discovery with a whitelist or a blacklist by
   setting `table.whitelist` or `table.blacklist` configuration.
4. By changing the types of tables to be considered by setting
   `table.types` configuration (e.g., to use database views).

The connector periodically polls the database metadata, detects created
and deleted tables. It automatically adapts to these changes by starting
polling created tables and stopping polling deleted ones. The metadata poll
interval is configured by `table.poll.interval.ms` and is 60 seconds by
default.

### Custom Query

Instead of relying on data source discovery, it is possible to set a
custom query to be executed by the connector. This may be useful if
joining tables or selecting a subset of columns is needed.

If the connector works in one of the
[incremental modes](#incremental-modes), it must be possible to append
`WHERE` clause to the query.

Currently, only one custom query at a time is supported.

#### Warning: Changing the Query And Stored Timestamps

With a custom query in an incremental mode, the processed timestamps are
stored independently on the query text itself. It means if the query is
modified in incompatible way (e.g., when a new query uses completely
different tables), the previously stored timestamps will still be used
on the next run of the connector.

It's possible to solve this either by manually deleting or altering the
stored offsets or by using a different connector name when the custom
query text is changed in an incompatible way.

## Kafka Topics

When the connector uses database tables as the data source, it maps each
table to the corresponding Kafka topic as `<prefix><table_name>`.

If the connector is configured to use a custom query, the topic name is
just `<prefix>`.

The prefix must be specified, set `topic.prefix=<prefix here>` for this.

## Query Modes

The connector has four query modes, the _bulk mode_, and three
_incremental modes_.

### Bulk Mode

In this mode, the connector will query tables without any filtering,
periodically retrieving all rows from them and publishing them to Kafka.

To use this mode, set `mode=bulk`.

### Incremental Modes

In these modes, the connector keeps track of the latest rows it has
already processed in each table and retrieves only rows that were added
or updated after those.

The incremental query modes use certain columns to discover created or
updated rows. To make polling done by the connector efficient, it is
advisable to have these columns indexed.

#### Incremental Mode With Incrementing Column

In this mode tables have a numeric column containing
sequentially growing numbers. Normally, this is a unique ID column with
automatically generated values, like `AUTO_INCREMENT` columns from MySQL
or sequential columns from PostgreSQL.

Normally updates do not change row IDs. Because of this, the connector
will detect only newly created rows. This makes this mode most suitable
for streaming immutable rows that are added to a table, for example, for
streaming facts from a fact table.

To use this mode, set `mode=incrementing`. Use 
`incrementing.column.name` for setting the incrementing column name.

#### Incremental Mode With Timestamp Column

In this mode tables have one or many timestamp columns.
The connector will apply `COALESCE` SQL function to them to get one
timestamp for a row. Rows with this timestamp greater than the largest
previously known will be retrieved on the next query.

An example use case might be streaming of rows that have `created_at`
and `updated_at` timestamps. When a row is created, it will be retrieved
for the first time. Later, when it is modified with updating the
`updated_at` timestamp, it will be retrieved again.

Sometimes it is necessary to introduce a delay between creation or
update of a row and its publishing to a topic. This delay can be set
using `timestamp.delay.interval.ms` configuration.

**Note:** Timestamps are not necessarily unique. This might cause the
following problematic situation. Two rows `R1` and `R2` share the same
timestamp `T` and have been retrieved from the database. However, only
`R1` has been written to a topic before a crash, but the timestamp `T`
has been persisted anyway. On the next poll, rows with timestamp `T`,
including `R2` will not be retrieved, leaving `R2` unprocessed. This
problem is addressed by the [incremental mode with incrementing and
timestamp columns](#incremental-mode-with-incrementing-and-timestamp-columns).

To use this mode, set `mode=timestamp`. Use `timestamp.column.name` for
setting the list of the timestamp column names.

#### Incremental Mode With Incrementing and Timestamp Columns

This mode is a combination of incremental modes with incrementing column
and timestamp columns, a more accurate and robust.

The mode is similar to the incremental mode with timestamp columns. The
only difference is that the controller in this mode uses incrementing
IDs along with timestamps to detect rows to be processed and processes
rows in the order of the timestamp _and_ the incrementing ID.

Unlike the incremental mode with timestamp columns, this mode is not
susceptible to the issue of a partial processing of rows with shared
timestamps.

Unlike the incremental mode with incrementing column, this mode allows
retrieving rows that have been updated.

To use this mode, set `mode=timestamp+incrementing`. Use
`timestamp.column.name` for setting the list of the timestamp column
names and `incrementing.column.name` for setting the incrementing column
name.

## Mapping Column Types

The connector maps SQL types to the most accurate representation in
Java.

### Mapping of `NUMERIC` And `DECIMAL`

`NUMERIC` and `DECIMAL` SQL types are naturally mapped into Connect
`Decimal` type (which is logically represented by Java `BigDecimal`
type). However, Avro serializes `Decimal`s as raw bytes, which may be
inconvenient to consume.

To fix this potential issue, the connector has several approaches to
mapping `NUMERIC` And `DECIMAL` to Connect types.

#### Always `Decimal`

This is the straightforward mapping from `NUMERIC` and `DECIMAL` SQL
types into `Decimal` Connect type.

This is the default behavior. To enable it explicitly, set
`numeric.mapping=none`.

#### Best Fit

In this mode, the connector will first try to find the appropriate
representation among the primitive Connect types before defaulting to
`Decimal`. The decision is based on the precision and scale.

| Precision |   Scale  | Connect "best fit" primitive type |
|:---------:|:--------:|:---------------------------------:|
|   1 to 2  | -84 to 0 |               `INT8`              |
|   3 to 4  | -84 to 0 |              `INT16`              |
|   5 to 9  | -84 to 0 |              `INT32`              |
|  10 to 18 | -84 to 0 |              `INT64`              |
|  1 to 18  | positive |             `FLOAT64`             |
|   other   |   other  |             `Decimal`             |

To use this mode, set `numeric.mapping=best_fit`.

#### Precision Only

In this mode, the connector will first try to find the appropriate
representation among the primitive Connect types before defaulting to
`Decimal`. The decision is based on the precision only, while the scale
is always 0.

| Precision | Scale | Connect "best fit" primitive type |
|:---------:|:-----:|:---------------------------------:|
|   1 to 2  |   0   |               `INT8`              |
|   3 to 4  |   0   |              `INT16`              |
|   5 to 9  |   0   |              `INT32`              |
|  10 to 18 |   0   |              `INT64`              |
|   other   | other |             `Decimal`             |

To use this mode, set `numeric.mapping=precision_only`.
