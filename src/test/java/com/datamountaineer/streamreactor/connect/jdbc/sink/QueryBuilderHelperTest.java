package com.datamountaineer.streamreactor.connect.jdbc.sink;

import com.datamountaineer.streamreactor.connect.jdbc.sink.config.FieldsMappings;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.FieldAlias;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkSettings;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.ErrorPolicyEnum;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.InsertModeEnum;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.InsertQueryBuilder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.QueryBuilder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.QueryBuilderHelper;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.UpsertQueryBuilder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect.MySqlDialect;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect.SQLiteDialect;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect.Sql2003Dialect;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect.SqlServerDialect;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class QueryBuilderHelperTest {
  @Test
  public void shouldCreateAnInsertStatement() {

    Map<String, FieldAlias> mappings = Maps.newHashMap();
    mappings.put("field1", new FieldAlias("field1"));
    mappings.put("field2", new FieldAlias("field2"));

    JdbcSinkSettings settings = new JdbcSinkSettings("jdbc:",
            null,
            null,
            Lists.newArrayList(new FieldsMappings("tableA", "topic", true, mappings)),
            true,
            ErrorPolicyEnum.NOOP,
            InsertModeEnum.INSERT,
            10);

    QueryBuilder queryBuilder = QueryBuilderHelper.from(settings);
    assertEquals(queryBuilder.getClass(), InsertQueryBuilder.class);
  }

  @Test
  public void shouldCreateAnInsertStatementWhenNoMappingsAreProvided() {

    Map<String, FieldAlias> mappings = Maps.newHashMap();

    JdbcSinkSettings settings = new JdbcSinkSettings("jdbc:",
            null,
            null,
            Lists.newArrayList(new FieldsMappings("tableA", "topic", true, mappings)),
            true,
            ErrorPolicyEnum.NOOP,
            InsertModeEnum.INSERT,
            10);

    QueryBuilder queryBuilder = QueryBuilderHelper.from(settings);
    assertEquals(queryBuilder.getClass(), InsertQueryBuilder.class);
  }

  @Test
  public void shouldCreateAnUpsertQueryBuilderWithSqlServerDialect() {
    Map<String, FieldAlias> mappings = Maps.newHashMap();
    mappings.put("field1", new FieldAlias("field1", true));
    mappings.put("field2", new FieldAlias("field2"));
    JdbcSinkSettings settings = new JdbcSinkSettings("jdbc:microsoft:sqlserver://HOST:1433;DatabaseName=DATABASE",
            null,
            null,
            Lists.newArrayList(new FieldsMappings("tableA", "topic", true, mappings)),
            true,
            ErrorPolicyEnum.NOOP,
            InsertModeEnum.UPSERT,
            10);

    QueryBuilder queryBuilder = QueryBuilderHelper.from(settings);
    assertEquals(queryBuilder.getClass(), UpsertQueryBuilder.class);

    UpsertQueryBuilder upsertQueryBuilder = (UpsertQueryBuilder) queryBuilder;
    assertEquals(upsertQueryBuilder.getDbDialect().getClass(), SqlServerDialect.class);
  }

  @Test
  public void shouldCreateAnUpsertQueryBuilderWithOracleDbDialect() {
    Map<String, FieldAlias> mappings = Maps.newHashMap();
    mappings.put("field1", new FieldAlias("field1", true));
    mappings.put("field2", new FieldAlias("field2"));
    JdbcSinkSettings settings = new JdbcSinkSettings("jdbc:oracle:thin:@localhost:1521:xe",
            null,
            null,
            Lists.newArrayList(new FieldsMappings("tableA", "topic", true, mappings)),
            true,
            ErrorPolicyEnum.NOOP,
            InsertModeEnum.UPSERT,
            10);

    QueryBuilder queryBuilder = QueryBuilderHelper.from(settings);
    assertEquals(queryBuilder.getClass(), UpsertQueryBuilder.class);

    UpsertQueryBuilder upsertQueryBuilder = (UpsertQueryBuilder) queryBuilder;
    assertEquals(upsertQueryBuilder.getDbDialect().getClass(), Sql2003Dialect.class);
  }

  @Test
  public void shouldCreateAnUpsertQueryBuilderWithMySqlDialect() {
    Map<String, FieldAlias> mappings = Maps.newHashMap();
    mappings.put("field1", new FieldAlias("field1", true));
    mappings.put("field2", new FieldAlias("field2"));
    JdbcSinkSettings settings = new JdbcSinkSettings("jdbc:mysql://HOST/DATABASE",
            null,
            null,
            Lists.newArrayList(new FieldsMappings("tableA", "topic", true, mappings)),
            true,
            ErrorPolicyEnum.NOOP,
            InsertModeEnum.UPSERT,
           10);

    QueryBuilder queryBuilder = QueryBuilderHelper.from(settings);
    assertEquals(queryBuilder.getClass(), UpsertQueryBuilder.class);

    UpsertQueryBuilder upsertQueryBuilder = (UpsertQueryBuilder) queryBuilder;
    assertEquals(upsertQueryBuilder.getDbDialect().getClass(), MySqlDialect.class);
  }

  @Test
  public void shouldCreateAnUpsertQueryBuilderWithSqLiteDialect() {
    Map<String, FieldAlias> mappings = Maps.newHashMap();
    mappings.put("field1", new FieldAlias("field1", true));
    mappings.put("field2", new FieldAlias("field2"));
    JdbcSinkSettings settings = new JdbcSinkSettings("jdbc:sqlite:/folder/db.file",
            null,
            null,
            Lists.newArrayList(new FieldsMappings("tableA", "topic", true, mappings)),
            true,
            ErrorPolicyEnum.NOOP,
            InsertModeEnum.UPSERT,
             10);

    QueryBuilder queryBuilder = QueryBuilderHelper.from(settings);
    assertEquals(queryBuilder.getClass(), UpsertQueryBuilder.class);

    UpsertQueryBuilder upsertQueryBuilder = (UpsertQueryBuilder) queryBuilder;
    assertEquals(upsertQueryBuilder.getDbDialect().getClass(), SQLiteDialect.class);
  }
}
