package com.pg.streams.repository;

import com.google.common.collect.Streams;
import com.pg.streams.PgProperties;
import com.pg.streams.api.CdcRepository;
import com.pg.streams.api.SchemaSupplier;
import com.pg.streams.model.CDCProcessingFields;
import com.pg.streams.model.RecordContainer;
import com.pg.streams.model.SchemaForTable;
import com.pg.streams.repository.paimoin.CreateCatalog;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnarRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.*;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.*;
import org.springframework.scheduling.annotation.Scheduled;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.pg.streams.model.CDCProcessingFields.HVR_ROWID;
import static com.pg.streams.model.CDCProcessingFields.INTERNAL_PREFIX;
import static com.pg.streams.util.ProcessorUtil.getHvrRowId;
import static java.util.function.Predicate.not;

@Slf4j
public class ApachePaimonRepository implements CdcRepository<List<RecordContainer>> {
    private final Catalog catalog;
    private final Map<String, Table> queries = new ConcurrentHashMap<>();
    private final SchemaSupplier schemaSupplier;
    private final Base64.Encoder enc = Base64.getEncoder();
    private final Base64.Decoder dec = Base64.getDecoder();
    private final PgProperties pgProperties;

    @SneakyThrows
    public ApachePaimonRepository(SchemaSupplier schemaSupplier, PgProperties pgProperties) {
        this.schemaSupplier = schemaSupplier;
        catalog = CreateCatalog.createFilesystemCatalog(pgProperties.stateDir());
        this.pgProperties = pgProperties;
        catalog.createDatabase("cdc_processor", true);
        var dbs = catalog.listDatabases();
        var tables = catalog.listTables("cdc_processor");
        log.info("Paimon Databases {} Tables: {}", dbs, tables);
    }

    @SneakyThrows
    @Override
    public List<Map<String, Object>> selectIn(String topicName, List<Record<String, GenericRecord>> records) {

        org.apache.avro.Schema schema = Optional.ofNullable(records)
                .filter(not(List::isEmpty))
                .map(r -> r.getFirst().value().getSchema())
                .orElse(null);

        List hvrRowIds = records.stream()
                .map(r -> getHvrRowId(r.headers()))
                .toList();

        return selectRowsFromTable(topicName, hvrRowIds, schema).toList();
    }

    @SneakyThrows
    private Stream<Map<String, Object>> selectRowsFromTable(String topicName, List hvrRowIds, org.apache.avro.Schema schema) {
        var table = getTable(topicName);
        var rowType = table.rowType();

        PredicateBuilder builder =
                new PredicateBuilder(rowType);

        Predicate in = builder.in(rowType.getFieldIndex(HVR_ROWID), hvrRowIds);

        ReadBuilder readBuilder =
                table.newReadBuilder()
                        .withFilter(List.of(in));

        var plan = readBuilder.newScan().plan();
        var read = readBuilder.newRead();
        try (RecordReader<InternalRow> reader = read.executeFilter()
                .createReader(plan)) {

            var batchIterator = reader
                    .transform(a -> internalRowReader(a, rowType))
                    .toCloseableIterator();
            return Streams.stream(batchIterator)
                    .map(e -> adjustKeyDataType(e, schema));
        }
    }

    private static Map<String, Object> internalRowReader(InternalRow a, RowType rowType) {
        Map<String, Object> row = new HashMap<>(a.getFieldCount());
        if (a instanceof ColumnarRow c) {
            for (int i = 0; i < c.getFieldCount(); i++) {
                var rt = rowType.getField(i);
                DataTypeRoot dataType = rt.type().getTypeRoot();
                switch (dataType) {
                    case INTEGER -> row.put(rt.name(), c.getInt(i));
                    case BIGINT -> row.put(rt.name(), c.getLong(i));
                    case VARCHAR -> row.put(rt.name(), c.getString(i).toString());
                    default -> throw new IllegalStateException("Unexpected value type: " + dataType);
                }
            }
        }
        return row;
    }

    @Override
    public SortedMap<Long, RecordContainer> selectUpdate(List<RecordContainer> updatedRecords) {
        if (updatedRecords.isEmpty()) {
            return new TreeMap<>();
        }

        var topicName = updatedRecords.getFirst().topicName();
        var schema = updatedRecords.getFirst().valueSchema();

        //db query execution
        var prevRowIds = updatedRecords.stream().map(RecordContainer::getPrevHvrRowId).distinct().toList();
        var previousRecordsByKey = selectRowsFromTable(topicName, prevRowIds, schema).collect(Collectors.toMap(row -> (Long) row.get(HVR_ROWID), row -> row));

        var result = new TreeMap<Long, RecordContainer>();
        //updates left join previousRecordsByKey on updates.prevHvrRowId = previousRecordsByKey.HvrRowId
        for (RecordContainer row : updatedRecords) {
            var previousRecord = Optional
                    .ofNullable(previousRecordsByKey.get(row.prevHvrRowId()))
                    .orElse(Map.of());

            var updatedRecordWithPreviousValues = row.withResult(previousRecord);
            result.merge(updatedRecordWithPreviousValues.hvrRowId(), updatedRecordWithPreviousValues, RecordContainer::addDuplicate);
        }
        return result;
    }

    @Override
    public List<RecordContainer> upsert(String topicName, List<RecordContainer> records) {
        var table = getTable(topicName).copy(Map.of(CoreOptions.WRITE_ONLY.key(), Boolean.TRUE.toString()));

        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();

        try (BatchTableWrite write = writeBuilder.newWrite()) {
            try (BatchTableCommit commit = writeBuilder.newCommit()) {
                List<DataField> fields = table.rowType()
                        .getFields();

                records.forEach(writeOneRow(fields, table, write));//do write
                List<CommitMessage> messages = write.prepareCommit();
                commit.commit(messages);
                return records;
            }
        } catch (Exception e) {
            throw new IllegalStateException("Problem during write to paimon", e);
        }
    }

    private Consumer<RecordContainer> writeOneRow(List<DataField> fields, Table table, TableWrite write) {
        return recordContainer -> {
            try {
                var record = recordContainer.record();
                var rowValue = record.value();
                var rowSchema = record.value().getSchema();
                if (log.isDebugEnabled()) {
                    log.debug("Schema in table {} -> {}", table.fullName(), rowSchema);
                }

                var row = new GenericRow(RowKind.INSERT, fields.size());
                int index = 0;
                for (DataField key : fields) {
                    Object object = getObject(key.name(), rowValue, rowSchema);
                    row.setField(index++, object);
                }
                write.write(row);
            } catch (Exception e) {
                throw new RuntimeException("Issue when writing to paimon", e);
            }
        };
    }


    @Scheduled(fixedRate = 10, timeUnit = TimeUnit.SECONDS)
    private void doCompact() {
        if (pgProperties.podName().equals("kafka-streams-cdc-0")) {
            queries.forEach((k, v) -> {
                log.info("Compaction started {}", k);
                var newOptions = new HashMap<>(v.options());
                newOptions.put(CoreOptions.SORT_SPILL_THRESHOLD.key(), "10");
                newOptions.put(CoreOptions.WRITE_ONLY.key(), Boolean.FALSE.toString());
                FileStoreTable tableCopy = (FileStoreTable) v.copy(newOptions);
                BatchWriteBuilder batchWriteBuilder = tableCopy.newBatchWriteBuilder();
                var commits = new ArrayList<CommitMessage>();
                for (int i = 0; i < tableCopy.bucketSpec().getNumBuckets(); i++) {
                    try (var writer = (TableWriteImpl) batchWriteBuilder.newWrite(); var ioManager = IOManager.create(pgProperties.stateDir() + "/tmp")) {
                        log.info("Compacting {} {}", i, k);
                        writer.withMemoryPool(new HeapMemorySegmentPool(tableCopy.coreOptions().writeBufferSize(), tableCopy.coreOptions().pageSize()));
                        writer.withBucketMode(BucketMode.HASH_FIXED);
                        writer.withIOManager(ioManager);
                        writer.compact(BinaryRow.EMPTY_ROW, i, false);
                        commits.addAll(writer.prepareCommit(true, Long.MAX_VALUE));
                        log.info("Compacted {} {}", i, k);
                    } catch (Exception e) {
                        log.warn("Error during compaction", e);
                        throw new RuntimeException(e);
                    }
                }
                try (var c = batchWriteBuilder.newCommit()) {
                    c.commit(commits);
                    log.info("Commits {}", commits);
                } catch (Exception e) {
                    log.warn("Error during compaction commit", e);
                }
                log.info("Compaction table finished {} {}", k, tableCopy.statistics().orElse(null));
            });
        }
    }

    @Override
    public void commit() {
        CdcRepository.super.commit();
    }

    private Object getObject(String fieldName, GenericRecord rowValue, org.apache.avro.Schema rowSchema) {
        Object o = rowValue.get(fieldName);
        var fieldType = rowSchema.getField(fieldName).schema().getType();
        var asStrValue = switch (fieldType) {
            case BYTES -> BinaryString.fromString(enc.encodeToString(((ByteBuffer) o).array()));
            case LONG -> {
                if (CDCProcessingFields.HVR_ROWID.equals(fieldName)) {
                    yield o;
                } else {
                    yield BinaryString.fromString(String.valueOf(o));
                }
            }
            default -> BinaryString.fromString(String.valueOf(o));
        };
        return asStrValue;
    }

    private Map<String, Object> adjustKeyDataType(Map<String, Object> keyRows, org.apache.avro.Schema schema) {
        if (Objects.isNull(schema)) {
            log.warn("Schema is null for {}", keyRows);
            return keyRows;
        } else {
            return keyRows.entrySet()
                    .stream()
                    .map(e -> mapTypes(e, schema))
                    .collect(Collector.of(
                            HashMap::new,
                            (map, t) -> map.put(t.getKey(), t.getValue()),
                            (map1, map2) -> {
                                map1.putAll(map2);
                                return map1;
                            }));
        }
    }

    private Map.Entry<String, Object> mapTypes(Map.Entry<String, Object> e, org.apache.avro.Schema schema) {
        if (e.getKey().startsWith(INTERNAL_PREFIX)) {
            return e;
        }
        if (e.getValue() instanceof String stringValue) {
            var field = schema.getField(e.getKey());
            var fieldType = field.schema().getType();
            var fieldWithRightType = switch (fieldType) {
                case BYTES -> ByteBuffer.wrap(dec.decode(stringValue)); //decode base64 to byte buffer
                case INT -> Integer.parseInt(stringValue);
                case LONG -> Long.parseLong(stringValue);
                default -> e.getValue();
            };
            return Map.entry(e.getKey(), fieldWithRightType);
        } else {
            return e;
        }
    }


    private Table getTable(String topicName) {
        return queries.computeIfAbsent(topicName, t -> {
            log.info("Create table in Paimon {} {}", catalog.warehouse(), t);
            var schemaForTopic = schemaSupplier.getSchemaFor(topicName);
            return createTable(schemaForTopic);
        });
    }


    private Table createTable(SchemaForTable schemaForTable) {
        try {
            Schema.Builder schemaBuilder = Schema.newBuilder();
            schemaBuilder.option(CoreOptions.FILE_FORMAT.key(), CoreOptions.FILE_FORMAT_ORC);
            schemaBuilder.option(CoreOptions.BUCKET.key(), "10");
//            schemaBuilder.option(CoreOptions.LOOKUP_WAIT.key(), "false");
            schemaBuilder.option(CoreOptions.WRITE_ONLY.key(), "true");
            schemaBuilder.column(CDCProcessingFields.HVR_ROWID, DataTypes.BIGINT());
            schemaForTable.keys().forEach(k -> schemaBuilder.column(k.name().toLowerCase(), DataTypes.VARCHAR(256)));
            schemaBuilder.primaryKey(CDCProcessingFields.HVR_ROWID);
            schemaBuilder.option(CoreOptions.FILE_INDEX + ".bloom-filter.columns", "hvr_rowid");
            Schema schema = schemaBuilder.build();
            Identifier identifier = Identifier.create("cdc_processor", schemaForTable.topicName());
            catalog.createTable(identifier, schema, true);
            return catalog.getTable(identifier);
        } catch (Exception e) {
            log.error("Some problem with creating table {}", schemaForTable, e);
            throw new RuntimeException(e);
        }
    }
}
