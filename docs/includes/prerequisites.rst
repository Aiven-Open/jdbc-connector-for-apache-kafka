.. Prerequisites for using JDBC connector

**Prerequisites:**

- :ref:`Confluent Platform <installation>` is installed and services are running by using the Confluent CLI. This quick start assumes that you are using the Confluent CLI, but standalone installations are also supported. By default ZooKeeper, Kafka, Schema Registry, Kafka Connect REST API, and Kafka Connect are started with the ``confluent start`` command. For more information, see :ref:`installation_archive`.
- `SQLite <https://sqlite.org/download.html>`_ is installed. You can also use another database. If you are using another database, be sure to adjust the ``connection.url`` setting. |CP| includes JDBC drivers for SQLite and PostgreSQL, but if you're using a different database you must also verify that the JDBC driver is available on the Kafka Connect process's ``CLASSPATH``.
- Kafka and Schema Registry are running locally on the default ports.


.. shared SQLite instructions

------------------------------------
Create SQLite Database and Load Data
------------------------------------

#.  Create a SQLite database with this command:

    .. sourcecode:: bash

       $ sqlite3 test.db

    Your output should resemble:

    .. sourcecode:: bash

       SQLite version 3.19.3 2017-06-27 16:48:08
       Enter ".help" for usage hints.
       sqlite>

#.  In the SQLite command prompt, create a table and seed it with some data:

    .. sourcecode:: bash

       sqlite> CREATE TABLE accounts(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, name VARCHAR(255));

    .. sourcecode:: bash

       sqlite> INSERT INTO accounts(name) VALUES('alice');

    .. sourcecode:: bash

       sqlite> INSERT INTO accounts(name) VALUES('bob');

    .. tip:: You can run ``SELECT * from accounts;`` to verify your table has been created.



