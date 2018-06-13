package org.sinmetal.spanner2bq;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.cloud.spanner.*;
import com.google.common.collect.Lists;
import javafx.util.Pair;

import java.util.*;
import java.util.logging.Logger;

public class SpannerUtil {

    static final Logger logger = Logger.getLogger(SpannerUtil.class.getSimpleName());

    public static  Map<String, List<TableFieldSchema>> getAllTableSchema(String project, String instance, String database) {
        SpannerOptions options = SpannerOptions.newBuilder().build();
        Spanner spanner = options.getService();

        List<Struct> resultsAsStruct = Lists.newArrayList();

        try {
            DatabaseId db = DatabaseId.of(project, instance, database);
            DatabaseClient databaseClient = spanner.getDatabaseClient(db);

            String sql = "SELECT" +
                    " t.table_name," +
                    " t.column_name," +
                    " t.ordinal_position," +
                    " t.column_default," +
                    " t.data_type," +
                    " t.is_nullable," +
                    " t.spanner_type" +
                    " FROM" +
                    " information_schema.columns AS t" +
                    " WHERE table_schema = \"\"" +
                    " ORDER BY" +
                    " t.table_catalog," +
                    " t.table_schema," +
                    " t.table_name," +
                    " t.ordinal_position";

            ResultSet resultSet = databaseClient.singleUse().executeQuery(Statement.of(sql));
            while (resultSet.next()) {
                resultsAsStruct.add(resultSet.getCurrentRowAsStruct());
            }
        } finally {
            spanner.close();
        }

        Map<String, List<TableFieldSchema>> tableSchemaMap = new HashMap<>();
        for (Struct struct : resultsAsStruct) {
            String tableName = struct.getString("table_name");
            if (!tableSchemaMap.containsKey(tableName)) {
                tableSchemaMap.put(tableName, new ArrayList<>());
            }

            List<TableFieldSchema> tableFieldSchemas = tableSchemaMap.get(tableName);
            String columnName = struct.getString("column_name");
            String spannerColumnType = struct.getString("spanner_type");
            tableFieldSchemas.add(createTableFieldSchema(columnName, spannerColumnType));
        }

        return tableSchemaMap;
    }

    protected static TableFieldSchema createTableFieldSchema(String columnName, String spannerColumnType) {
        TableFieldSchema tableFieldSchema = new TableFieldSchema();
        tableFieldSchema.setName(columnName);
        Pair<String, String> bigQueryColumnType = getBigQueryColumnType(spannerColumnType);
        tableFieldSchema.setType(bigQueryColumnType.getKey());
        tableFieldSchema.setMode(bigQueryColumnType.getValue());
        return tableFieldSchema;
    }

    protected static Pair<String, String> getBigQueryColumnType(String spannerColumnType) {
        if (spannerColumnType.startsWith("ARRAY")) {
            String arrayType = spannerColumnType.substring("ARRAY<".length());
            Pair<String, String> bigQueryColumnType = getBigQueryColumnType(arrayType);
            return new Pair<>(bigQueryColumnType.getKey(), "REPEATED");
        };
        if (spannerColumnType.startsWith("STRING")) {
            return new Pair<>("STRING", "NULLABLE");
        }
        if (spannerColumnType.startsWith("INT64")) {
            return new Pair<>("INT64", "NULLABLE");
        }
        if (spannerColumnType.startsWith("FLOAT64")) {
            return new Pair<>("FLOAT64", "NULLABLE");
        }
        if (spannerColumnType.startsWith("BOOL")) {
            return new Pair<>("BOOL", "NULLABLE");
        }
        if (spannerColumnType.startsWith("BYTES")) {
            return new Pair<>("BYTES", "NULLABLE");
        }
        if (spannerColumnType.startsWith("DATE")) {
            return new Pair<>("DATE", "NULLABLE");
        }
        if (spannerColumnType.startsWith("TIMESTAMP")) {
            return new Pair<>("TIMESTAMP", "NULLABLE");
        }

        throw new IllegalArgumentException(spannerColumnType + " is not supported column type.");
    }
}
