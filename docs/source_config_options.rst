JDBC Source Configuration Options
---------------------------------

Database
^^^^^^^^

``connection.url``
  JDBC connection URL for the database to load.

  * Type: string
  * Importance: high
  * Dependents: ``table.whitelist``, ``table.blacklist``

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

``schema.pattern``
  Schema pattern to fetch tables metadata from the database:

    * "" retrieves those without a schema,  * null (default) means that the schema name should not be used to narrow the search, all tables metadata would be fetched, regardless their schema.

  * Type: string
  * Default: null
  * Importance: medium

Connector
^^^^^^^^^

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

``topic.prefix``
  Prefix to prepend to table names to generate the name of the Kafka topic to publish data to, or in the case of a custom query, the full name of the topic to publish to.

  * Type: string
  * Importance: high

``timestamp.delay.interval.ms``
  How long to wait after a row with certain timestamp appears before we include it in the result. You may choose to add some delay to allow transactions with earlier timestamp to complete. The first execution will fetch all available records (i.e. starting at timestamp 0) until current time minus the delay. Every following execution will get data from the last time we fetched until current time minus the delay.

  * Type: long
  * Default: 0
  * Importance: high

Mode
^^^^

``mode``
  The mode for updating a table each time it is polled. Options include:

    * bulk - perform a bulk load of the entire table each time it is polled

    * incrementing - use a strictly incrementing column on each table to detect only new rows. Note that this will not detect modifications or deletions of existing rows.

    * timestamp - use a timestamp (or timestamp-like) column to detect new and modified rows. This assumes the column is updated with each write, and that values are monotonically incrementing, but not necessarily unique.

    * timestamp+incrementing - use two columns, a timestamp column that detects new and modified rows and a strictly incrementing column which provides a globally unique ID for updates so each row can be assigned a unique stream offset.

  * Type: string
  * Default: ""
  * Valid Values: [, bulk, timestamp, incrementing, timestamp+incrementing]
  * Importance: high
  * Dependents: ``incrementing.column.name``, ``timestamp.column.name``, ``validate.non.null``

``incrementing.column.name``
  The name of the strictly incrementing column to use to detect new rows. Any empty value indicates the column should be autodetected by looking for an auto-incrementing column. This column may not be nullable.

  * Type: string
  * Default: ""
  * Importance: medium

``timestamp.column.name``
  The name of the timestamp column to use to detect new or modified rows. This column may not be nullable.

  * Type: string
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
