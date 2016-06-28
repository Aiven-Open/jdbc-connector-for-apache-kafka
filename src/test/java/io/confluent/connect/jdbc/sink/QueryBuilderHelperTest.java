package io.confluent.connect.jdbc.sink;

import org.junit.Assert;
import org.junit.Test;

import io.confluent.connect.jdbc.sink.config.InsertModeEnum;
import io.confluent.connect.jdbc.sink.dialect.MySqlDialect;
import io.confluent.connect.jdbc.sink.dialect.OracleDialect;
import io.confluent.connect.jdbc.sink.dialect.SQLiteDialect;
import io.confluent.connect.jdbc.sink.dialect.SqlServerDialect;
import io.confluent.connect.jdbc.sink.writer.InsertQueryBuilder;
import io.confluent.connect.jdbc.sink.writer.QueryBuilder;
import io.confluent.connect.jdbc.sink.writer.QueryBuilderHelper;
import io.confluent.connect.jdbc.sink.writer.UpsertQueryBuilder;

import static org.junit.Assert.assertEquals;

public class QueryBuilderHelperTest {
  @Test
  public void shouldCreateAnInsertStatement() {
    QueryBuilder queryBuilder = QueryBuilderHelper.from("jdbc:jtds:sqlserver://aa:", InsertModeEnum.INSERT);
    assertEquals(queryBuilder.getClass(), InsertQueryBuilder.class);
  }

  @Test
  public void shouldCreateAnInsertStatementWhenNoMappingsAreProvided() {
    QueryBuilder queryBuilder = QueryBuilderHelper.from("jdbc:jtds:sqlserver://aa:1241", InsertModeEnum.INSERT);
    assertEquals(queryBuilder.getClass(), InsertQueryBuilder.class);
  }

  @Test
  public void shouldCreateAnUpsertQueryBuilderWithSqlServerDialect() {
    QueryBuilder queryBuilder = QueryBuilderHelper.from("jdbc:microsoft:sqlserver://HOST:1433;DatabaseName=DATABASE",
                                                        InsertModeEnum.UPSERT);
    assertEquals(queryBuilder.getClass(), UpsertQueryBuilder.class);

    UpsertQueryBuilder upsertQueryBuilder = (UpsertQueryBuilder) queryBuilder;
    Assert.assertEquals(upsertQueryBuilder.getDbDialect().getClass(), SqlServerDialect.class);
  }

  @Test
  public void shouldCreateAnUpsertQueryBuilderWithOracleDbDialect() {
    QueryBuilder queryBuilder = QueryBuilderHelper.from("jdbc:oracle:thin:@localhost:1521:xe", InsertModeEnum.UPSERT);
    assertEquals(queryBuilder.getClass(), UpsertQueryBuilder.class);

    UpsertQueryBuilder upsertQueryBuilder = (UpsertQueryBuilder) queryBuilder;
    Assert.assertEquals(upsertQueryBuilder.getDbDialect().getClass(), OracleDialect.class);
  }

  @Test
  public void shouldCreateAnUpsertQueryBuilderWithMySqlDialect() {

    QueryBuilder queryBuilder = QueryBuilderHelper.from("jdbc:mysql://HOST/DATABASE", InsertModeEnum.UPSERT);
    assertEquals(queryBuilder.getClass(), UpsertQueryBuilder.class);

    UpsertQueryBuilder upsertQueryBuilder = (UpsertQueryBuilder) queryBuilder;
    Assert.assertEquals(upsertQueryBuilder.getDbDialect().getClass(), MySqlDialect.class);
  }

  @Test
  public void shouldCreateAnUpsertQueryBuilderWithSqLiteDialect() {
    QueryBuilder queryBuilder = QueryBuilderHelper.from("jdbc:sqlite:/folder/db.file", InsertModeEnum.UPSERT);
    assertEquals(queryBuilder.getClass(), UpsertQueryBuilder.class);

    UpsertQueryBuilder upsertQueryBuilder = (UpsertQueryBuilder) queryBuilder;
    Assert.assertEquals(upsertQueryBuilder.getDbDialect().getClass(), SQLiteDialect.class);
  }
}
