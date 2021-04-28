===========================================
JDBC Source connector Configuration Options
===========================================

Database
^^^^^^^^

``connection.url``
  JDBC connection URL.

  * Type: string
  * Importance: high
  * Dependents: ``table.whitelist``, ``table.blacklist``

``connection.user``
  JDBC connection user.

  * Type: string
  * Default: null
  * Importance: high

``connection.password``
  JDBC connection password.

  * Type: password
  * Default: null
  * Importance: high

``connection.attempts``
  Maximum number of attempts to retrieve a valid JDBC connection.

  * Type: int
  * Default: 3
  * Importance: low

``connection.backoff.ms``
  Backoff time in milliseconds between connection attempts.

  * Type: long
  * Default: 10000
  * Importance: low

``table.whitelist``
  List of tables to include in copying. If specified, table.blacklist may not be set.

  * Type: list
  * Default: ""
  * Importance: medium

``table.blacklist``
  List of tables to exclude from copying. If specified, table.whitelist may not be set.

  * Type: list
  * Default: ""
  * Importance: medium

``catalog.pattern``
  Catalog pattern to fetch table metadata from the database:

    * "" retrieves those without a catalog,

    * null (default) means that the catalog name should not be used to narrow the search so that all table metadata would be fetched, regardless of their catalog.

  * Type: string
  * Default: null
  * Importance: medium

``schema.pattern``
  Schema pattern to fetch table metadata from the database:

    * "" retrieves those without a schema,

    * null (default) means that the schema name should not be used to narrow the search, so that all table metadata would be fetched, regardless of their schema.

  * Type: string
  * Default: null
  * Importance: medium

``numeric.precision.mapping``
  Whether or not to attempt mapping NUMERIC values by precision to integral types. This option is now deprecated. A future version may remove it completely. Please use ``numeric.mapping`` instead.

  * Type: boolean
  * Default: false
  * Importance: low

``numeric.mapping``
  Map NUMERIC values by precision and optionally scale to integral or decimal types. Use ``none`` if all NUMERIC columns are to be represented by Connect's DECIMAL logical type. Use ``best_fit`` if NUMERIC columns should be cast to Connect's INT8, INT16, INT32, INT64, or FLOAT64 based upon the column's precision and scale. Or use ``precision_only`` to map NUMERIC columns based only on the column's precision assuming that column's scale is 0. The ``none`` option is the default, but may lead to serialization issues with Avro since Connect's DECIMAL type is mapped to its binary representation, and ``best_fit`` will often be preferred since it maps to the most appropriate primitive type.

  * Type: string
  * Default: null
  * Valid Values: [none, precision_only, best_fit]
  * Importance: low

``db.timezone``
  Name of the JDBC timezone that should be used in the connector when querying with time-based criteria. Defaults to UTC.

  * Type: string
  * Default: UTC
  * Valid Values: valid time zone identifier (e.g., 'Europe/Helsinki', 'UTC+2', 'Z', 'CET')
  * Importance: medium

``dialect.name``
  The name of the database dialect that should be used for this connector. By default this is empty, and the connector automatically determines the dialect based upon the JDBC connection URL. Use this if you want to override that behavior and use a specific dialect. All properly-packaged dialects in the JDBC connector plugin can be used.

  * Type: string
  * Default: ""
  * Valid Values: [, Db2DatabaseDialect, MySqlDatabaseDialect, SybaseDatabaseDialect, GenericDatabaseDialect, OracleDatabaseDialect, SqlServerDatabaseDialect, PostgreSqlDatabaseDialect, SqliteDatabaseDialect, DerbyDatabaseDialect, SapHanaDatabaseDialect, VerticaDatabaseDialect]
  * Importance: low

``sql.quote.identifiers``
  Whether to delimit (in most databases, quote with double quotes) identifiers (e.g., table names and column names) in SQL statements.

  * Type: boolean
  * Default: true
  * Importance: low

Mode
^^^^

``mode``
  The mode for updating a table each time it is polled. Options include:

    * bulk - perform a bulk load of the entire table each time it is polled

    * incrementing - use a strictly incrementing column on each table to detect only new rows. Note that this will not detect modifications or deletions of existing rows.

    * timestamp - use a timestamp (or timestamp-like) column to detect new and modified rows. This assumes the column is updated with each write, and that values are monotonically incrementing, but not necessarily unique.

    * timestamp+incrementing - use two columns, a timestamp column that detects new and modified rows and a strictly incrementing column which provides a globally unique ID for updates so each row can be assigned a unique stream offset.

  * Type: string
  * Valid Values: [bulk, timestamp, incrementing, timestamp+incrementing]
  * Importance: high
  * Dependents: ``incrementing.column.name``, ``timestamp.column.name``, ``validate.non.null``

``incrementing.column.name``
  The name of the strictly incrementing column to use to detect new rows. Any empty value indicates the column should be autodetected by looking for an auto-incrementing column. This column may not be nullable.

  * Type: string
  * Default: ""
  * Importance: medium

``timestamp.column.name``
  Comma separated list of one or more timestamp columns to detect new or modified rows using the COALESCE SQL function. Rows whose first non-null timestamp value is greater than the largest previous timestamp value seen will be discovered with each poll. At least one column should not be nullable.

  * Type: list
  * Default: ""
  * Importance: medium

``validate.non.null``
  By default, the JDBC connector will validate that all incrementing and timestamp tables have NOT NULL set for the columns being used as their ID/timestamp. If the tables don't, JDBC connector will fail to start. Setting this to false will disable these checks.

  * Type: boolean
  * Default: true
  * Importance: low

``query``
  If specified, the query to perform to select new or updated rows. Use this setting if you want to join tables, select subsets of columns in a table, or filter data. If used, this connector will only copy data using this query -- whole-table copying will be disabled. Different query modes may still be used for incremental updates, but in order to properly construct the incremental query, it must be possible to append a WHERE clause to this query (i.e. no WHERE clauses may be used). If you use a WHERE clause, it must handle incremental queries itself.

  * Type: string
  * Default: ""
  * Importance: medium

Connector
^^^^^^^^^

``table.types``
  By default, the JDBC connector will only detect tables with type TABLE from the source Database. This config allows a command separated list of table types to extract. Options include:

  * TABLE

  * VIEW

  * SYSTEM TABLE

  * GLOBAL TEMPORARY

  * LOCAL TEMPORARY

  * ALIAS

  * SYNONYM

  In most cases it only makes sense to have either TABLE or VIEW.

  * Type: list
  * Default: TABLE
  * Importance: low

``poll.interval.ms``
  Frequency in ms to poll for new data in each table.

  * Type: int
  * Default: 5000
  * Importance: high

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

``topic.prefix``
  Prefix to prepend to table names to generate the name of the Kafka topic to publish data to, or in the case of a custom query, the full name of the topic to publish to.

  * Type: string
  * Importance: high

``timestamp.delay.interval.ms``
  How long to wait after a row with certain timestamp appears before we include it in the result. You may choose to add some delay to allow transactions with earlier timestamp to complete. The first execution will fetch all available records (i.e. starting at timestamp greater than 0) until current time minus the delay. Every following execution will get data from the last time we fetched until current time minus the delay.

  * Type: long
  * Default: 0
  * Importance: high

``timestamp.initial.ms``
  The initial value of timestamp when selecting records. The records having timestamp greater than the value are included in the result.

  * Type: long
  * Default: 0
  * Importance: medium

``incrementing.initial``
  The initial value of incremental column when selecting records. The records the incremental column with value greater than the configured value are included in the result.

  * Type: long
  * Default: -1
  * Importance: medium

