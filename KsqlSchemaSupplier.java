package com.pg.streams.repository.schema;

import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.pg.streams.PgProperties;
import com.pg.streams.api.SchemaSupplier;
import com.pg.streams.exceptions.NoSchemaFoundException;
import com.pg.streams.model.SchemaForTable;
import com.pg.streams.model.SchemaForTableField;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.Row;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Implementation of the SchemaSupplier interface for supplying schema information
 * using ksqlDB. This class interacts with a ksqlDB server to fetch and supply schema
 * details for specified topics.
 */
@Slf4j
public class KsqlSchemaSupplier implements SchemaSupplier {

    private final ClientOptions options;
    private final KsqlClientFactory ksqlClientFactory;

    public KsqlSchemaSupplier(PgProperties pgProperties, KsqlClientFactory ksqlClientFactory) {
        this.ksqlClientFactory = ksqlClientFactory;
        options = ClientOptions.create()
                .setHost(pgProperties.ksqlUrl())
                .setUseTls(true).setVerifyHost(false).setUseAlpn(true)
                .setTrustStore(pgProperties.srTruststore())
                .setTrustStorePassword(pgProperties.sslPassword())
                .setExecuteQueryMaxResultRows(10000)
                .setPort(pgProperties.ksqlPort());

        if (Objects.nonNull(pgProperties.ksqlUserName()) && !pgProperties.ksqlUserName().isEmpty()) {
            options.setBasicAuthCredentials(pgProperties.ksqlUserName(), pgProperties.ksqlPassword());
        }
    }

    /**
     * Retrieves the schema information for the specified list of topics from the ksqlDB.
     * The method constructs a SQL query based on the provided topics, executes it against the ksqlDB,
     * and parses the result to build a list of SchemaForTable objects.
     *
     * @param topics the list of topics for which the schema information is to be retrieved
     * @return a list of SchemaForTable objects containing schema information for the specified topics
     */
    @Override
    @SneakyThrows
    public List<SchemaForTable> getSchemaFor(List<String> topics) {
        var queries = Lists.partition(topics, 200)
                .stream()
                .map(ts -> ts.stream().map(t -> "'" + t + "'").collect(Collectors.joining(",")))
                .map(ts -> "select * from RTD_KSQL_ALL_BUSINESS_TABLE_COLUMN_KEYS_BY_TOPIC where ROWKEY IN (" + ts + ");")
                .toList();

        var schemas = new ArrayList<SchemaForTable>();
        int totalQueries = queries.size();  // Extract variable for readability
        var counter = 1;

        for (String query : queries) {
            log.info("Query {}/{} for KEY {}", counter++, totalQueries, query);

            try (var client = ksqlClientFactory.create(options)) {
                var futureQueryResult = client.executeQuery(query);

                futureQueryResult.get(15, TimeUnit.SECONDS).stream()
                        .map(this::mapToSchemaForTable)
                        .forEach(schemas::add);
            }
        }
        log.info("Found schemas {}", schemas.size());
        return schemas;
    }

    /**
     * Maps a given row to a SchemaForTable object.
     *
     * @param r The row containing the data to map.
     * @return A SchemaForTable object constructed from the provided row.
     */
    private SchemaForTable mapToSchemaForTable(Row r) {
        var allCols = Streams
                .stream(r.getKsqlArray("PRIMARY_KEYS_NAME").stream().iterator())
                .map(String.class::cast)
                .distinct()
                .map(this::createSchemaForTableField)
                .toList();
        
        return new SchemaForTable(r.getString("ROWKEY"), allCols);
    }

    /**
     * Creates a schema definition for a table field with the specified column name.
     *
     * @param column the name of the column for which the schema is being created
     * @return a SchemaForTableField instance with the specified column name and a default data type of "Varchar(128)"
     */
    private SchemaForTableField createSchemaForTableField(String column) {
        return new SchemaForTableField(column, "Varchar(128)");
    }

    /**
     * Retrieves the schema for a given topic.
     *
     * @param topic the name of the topic for which the schema is being requested.
     * @return the schema associated with the specified topic.
     * @throws NoSchemaFoundException if no schema is found for the specified topic.
     */
    @Override
    @SneakyThrows
    public SchemaForTable getSchemaFor(String topic) {
        List<SchemaForTable> schema = getSchemaFor(List.of(topic));
        if (schema.isEmpty()) {
            throw new NoSchemaFoundException(topic);
        } else {
            return schema.getFirst();
        }
    }

    /**
     * Evaluates the given list of topics and checks whether schemas exist for them.
     *
     * @param topics a List of topic names to be checked against existing schemas.
     * @return a Map where the key is a Boolean indicating the presence of a schema (true if present, false otherwise),
     *         and the value is a List of topic names.
     */
    @Override
    public Map<Boolean, List<String>> checkSchemas(List<String> topics) {
        var foundSchemas = getSchemaFor(topics).stream().map(SchemaForTable::topicName).collect(Collectors.toSet());

        return topics.stream()
                .map(e -> Map.entry(foundSchemas.contains(e), e))
                .collect(Collectors.groupingBy(Map.Entry::getKey,
                        Collectors.mapping(Map.Entry::getValue,
                                Collectors.toList())));
    }

    interface KsqlClientFactory {
        Client create(ClientOptions clientOptions);
    }

    public static class KsqlClientFactoryImpl implements KsqlClientFactory {
        @Override
        public Client create(ClientOptions clientOptions) {
            return Client.create(clientOptions);
        }
    }
}
