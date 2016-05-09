package com.datamountaineer.streamreactor.connect.jdbc.sink;


import com.datamountaineer.streamreactor.connect.FieldAlias;
import com.datamountaineer.streamreactor.connect.config.PayloadFields;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.ErrorPolicyEnum;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkSettings;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.InsertQueryBuilder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.QueryBuilder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.QueryBuilderHelper;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.UpsertQueryBuilder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect.DbDialectTypeEnum;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect.MySqlDialect;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect.SQLiteDialect;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect.Sql2003Dialect;
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

        JdbcSinkSettings settings = new JdbcSinkSettings("jdbc:", "tableA",
                new PayloadFields(true, mappings),
                true,
                ErrorPolicyEnum.NOOP,
                DbDialectTypeEnum.NONE);

        QueryBuilder queryBuilder = QueryBuilderHelper.from(settings);
        assertEquals(queryBuilder.getClass(), InsertQueryBuilder.class);
    }

    @Test
    public void shouldCreateAnInsertStatementWhenNoMappingsAreProvided() {

        Map<String, FieldAlias> mappings = Maps.newHashMap();

        JdbcSinkSettings settings = new JdbcSinkSettings("jdbc:", "tableA",
                new PayloadFields(true, mappings),
                true,
                ErrorPolicyEnum.NOOP,
                DbDialectTypeEnum.NONE);

        QueryBuilder queryBuilder = QueryBuilderHelper.from(settings);
        assertEquals(queryBuilder.getClass(), InsertQueryBuilder.class);
    }

    @Test
    public void shouldCreateAnUpsertQueryBuilderWhenPrimaryKeysAreProvidedWithASql2003Dialect() {
        Map<String, FieldAlias> mappings = Maps.newHashMap();
        mappings.put("field1", new FieldAlias("field1", true));
        mappings.put("field2", new FieldAlias("field2"));
        JdbcSinkSettings settings = new JdbcSinkSettings("jdbc:", "tableA",
                new PayloadFields(true, mappings),
                true,
                ErrorPolicyEnum.NOOP,
                DbDialectTypeEnum.MSSQL);

        QueryBuilder queryBuilder = QueryBuilderHelper.from(settings);
        assertEquals(queryBuilder.getClass(), UpsertQueryBuilder.class);

        UpsertQueryBuilder upsertQueryBuilder = (UpsertQueryBuilder) queryBuilder;
        assertEquals(upsertQueryBuilder.getDbDialect().getClass(), Sql2003Dialect.class);
    }


    @Test
    public void shouldCreateAnUpsertQueryBuilderWhenPrimaryKeysAreProvidedWithAMySqlDialect() {
        Map<String, FieldAlias> mappings = Maps.newHashMap();
        mappings.put("field1", new FieldAlias("field1", true));
        mappings.put("field2", new FieldAlias("field2"));
        JdbcSinkSettings settings = new JdbcSinkSettings("jdbc:", "tableA",
                new PayloadFields(true, mappings),
                true,
                ErrorPolicyEnum.NOOP,
                DbDialectTypeEnum.MYSQL);

        QueryBuilder queryBuilder = QueryBuilderHelper.from(settings);
        assertEquals(queryBuilder.getClass(), UpsertQueryBuilder.class);

        UpsertQueryBuilder upsertQueryBuilder = (UpsertQueryBuilder) queryBuilder;
        assertEquals(upsertQueryBuilder.getDbDialect().getClass(), MySqlDialect.class);
    }

    @Test
    public void shouldCreateAnUpsertQueryBuilderWhenPrimaryKeysAreProvidedWithASqLitDialect() {
        Map<String, FieldAlias> mappings = Maps.newHashMap();
        mappings.put("field1", new FieldAlias("field1", true));
        mappings.put("field2", new FieldAlias("field2"));
        JdbcSinkSettings settings = new JdbcSinkSettings("jdbc:", "tableA",
                new PayloadFields(true, mappings),
                true,
                ErrorPolicyEnum.NOOP,
                DbDialectTypeEnum.SQLITE);

        QueryBuilder queryBuilder = QueryBuilderHelper.from(settings);
        assertEquals(queryBuilder.getClass(), UpsertQueryBuilder.class);

        UpsertQueryBuilder upsertQueryBuilder = (UpsertQueryBuilder) queryBuilder;
        assertEquals(upsertQueryBuilder.getDbDialect().getClass(), SQLiteDialect.class);
    }
}
