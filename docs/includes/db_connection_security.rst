Database Connection Security
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In the connector configuration you will notice there are no security parameters. This is because
SSL is not part of the JDBC standard and will depend on the JDBC driver in use. In general, you
will need to configure SSL via the ``connection.url`` parameter. For example, with MySQL it would
look like:

.. sourcecode:: properties

    connection.url="jdbc:mysql://127.0.0.1:3306/sample?verifyServerCertificate=false&useSSL=true&requireSSL=true"

Please check with your specific JDBC driver documentation on support and configuration.
