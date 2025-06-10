package com.pg.streams.repository;

import com.google.common.collect.Streams;
import com.pg.streams.PgProperties;
import com.pg.streams.api.SchemaSupplier;
import com.pg.streams.api.model.HVROperations;
import com.pg.streams.model.RecordContainer;
import com.pg.streams.model.SchemaForTable;
import com.pg.streams.model.SchemaForTableField;
import lombok.SneakyThrows;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnarRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataTypeRoot;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.pg.streams.model.CDCProcessingFields.*;

class ApachePaimonRepositoryTest {

    @Test
    public void allWrittenDataShouldBeRead() {
        //given
        var schemaSupplierMock = Mockito.mock(SchemaSupplier.class);

        Mockito.when(schemaSupplierMock.getSchemaFor(Mockito.anyString())).thenAnswer(a -> new SchemaForTable(a.getArgument(0), List.of(
                new SchemaForTableField("K1", "STRING"),
                new SchemaForTableField("K2", "STRING")
        )));

        var repo = new ApachePaimonRepository(schemaSupplierMock, PgProperties.buildDefault().withStateDir("mock_catalog"));

        //IN
        String testTableName = "foo";
        var fixtures = IntStream.range(1, 10)
                .mapToObj(i -> RecordContainer.create(i, buildEmptyRecord("RK1", 10L + i, "123"), testTableName, HVROperations.INSERT, 9L + i))
                .toList();

        //when
        var actual = repo.upsert(testTableName, fixtures).stream().map(RecordContainer::valueAsMap).toList();
        var expected = readTable("mock_catalog", "cdc_processor", testTableName);
        Assertions.assertThat(actual).containsAll(expected);
    }


    @Test
    public void allDeletesShouldHaveKeys() {
        //given
        var schemaSupplierMock = Mockito.mock(SchemaSupplier.class);

        Mockito.when(schemaSupplierMock.getSchemaFor(Mockito.anyString())).thenAnswer(a -> new SchemaForTable(a.getArgument(0), List.of(
                new SchemaForTableField("K1", "STRING"),
                new SchemaForTableField("K2", "STRING")
        )));

        var repo = new ApachePaimonRepository(schemaSupplierMock, PgProperties.buildDefault().withStateDir("mock_catalog"));

        //IN
        var fixtures = IntStream.range(0, 10)
                .mapToObj(i -> RecordContainer.create(i, buildEmptyRecord("RK1", 10L + i, "123"), "foo", HVROperations.INSERT, 9L + i))
                .toList();
        String testTableName = "foo1";
        repo.upsert(testTableName, fixtures);

        var deletes = IntStream.range(0, 5)
                .mapToObj(i -> RecordContainer.create(i, buildEmptyRecord("RK1", 10L + i, "123"), "foo", HVROperations.DELETE, 10L + i))
                .toList();
        //when
        var actual = repo.selectIn(testTableName, deletes.stream().map(RecordContainer::record).toList());

        //then
        var expected = deletes.stream().map(d -> d.valueAsMap()).toList();
        Assertions.assertThat(actual).containsAll(expected);
    }


    @Test
    public void allUpdatesShouldHaveKeys() {
        //given
        String testTableName = "foo2";
        var schemaSupplierMock = Mockito.mock(SchemaSupplier.class);

        Mockito.when(schemaSupplierMock.getSchemaFor(Mockito.anyString())).thenAnswer(a -> new SchemaForTable(a.getArgument(0), List.of(
                new SchemaForTableField("K1", "STRING"),
                new SchemaForTableField("K2", "STRING")
        )));

        var repo = new ApachePaimonRepository(schemaSupplierMock, PgProperties.buildDefault().withStateDir("mock_catalog"));

        //IN
        var fixtures = IntStream.range(0, 10)
                .mapToObj(i -> RecordContainer.create(i, buildEmptyRecord("RK1", 10L + i, "123"), "foo", HVROperations.INSERT, 9L + i))
                .toList();
        repo.upsert(testTableName, fixtures);

        var updates = IntStream.range(0, 5)
                .mapToObj(i -> RecordContainer.create(i, buildEmptyRecord("RK1", 20L + i, "123"), "foo", HVROperations.UPDATE, 10L + i))
                .toList();
        //when
        var actual = repo.selectUpdate(updates);

        //then
        var expected = updates.stream().map(RecordContainer::result).toList();
        Assertions.assertThat(actual.values().stream().map(RecordContainer::result).toList()).containsAll(expected);
    }

    private Record buildEmptyRecord(String key, Long hvrRowId, String seq) {
        var sch = SchemaBuilder.builder("com.pg.test")
                .record("rec")
                .fields()
                .requiredLong(HVR_ROWID)
                .requiredString("k1")
                .requiredString("k2")
                .endRecord();
        var rb = new GenericRecordBuilder(sch);
        var headers = new RecordHeaders();
        headers.add(new RecordHeader(TOPIC_NAME, "orgTopic".getBytes()));
        headers.add(new RecordHeader(USE_DLQ, "false".getBytes()));
        headers.add(new RecordHeader(TX, "TX1".getBytes()));
        headers.add(new RecordHeader(SEQ, seq.getBytes()));
        headers.add(new RecordHeader(HVR_ROWID, String.valueOf(hvrRowId).getBytes()));
        return new Record<String, GenericRecord>(key, rb.set(HVR_ROWID, hvrRowId).set("k1", "K1").set("k2", "K2").build(), 1L, headers);
    }

    @SneakyThrows
    public static List<Map<String, Object>> readTable(String catalog, String dbName, String tableName) {
        Identifier identifier = Identifier.create(dbName, tableName);

        try (Catalog catalog1 = CatalogFactory.createCatalog(CatalogContext.create(new Path(catalog)))) {
            Table table = catalog1.getTable(identifier);
            ReadBuilder readBuilder = table.newReadBuilder();

            var plan = readBuilder.newScan().plan();
            TableRead read = readBuilder.newRead();
            org.apache.paimon.utils.CloseableIterator<Map<String, Object>> batchIterator;
            try (RecordReader<InternalRow> reader = read
                    .executeFilter()
                    .createReader(plan)) {

                var rowType = table.rowType();

                batchIterator = reader.transform(a -> {
                    Map<String, Object> row = new HashMap<>(a.getFieldCount());
                    if (a instanceof ColumnarRow c) {
                        for (int i = 0; i < c.getFieldCount(); i++) {
                            var rt = rowType.getField(i);
                            DataTypeRoot dataType = rt.type().getTypeRoot();
                            switch (dataType) {
                                case INTEGER -> row.put(rt.name(), c.getInt(i));
                                case BIGINT -> row.put(rt.name(), c.getLong(i));
                                case VARCHAR -> row.put(rt.name(), c.getString(i).toString());
                                default -> throw new IllegalStateException("Unexpected value: " + dataType);
                            }
                        }
                    }
                    return row;
                }).toCloseableIterator();
                return Streams.stream(batchIterator).toList();
            }
        }
    }
}