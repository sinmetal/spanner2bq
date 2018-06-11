package org.sinmetal.spanner2bq;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.spanner.Struct;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class SpannerToBigQuery {

    static final Logger logger = Logger.getLogger(SpannerToBigQuery.class.getSimpleName());

    public interface SpannerToBigQueryOptions extends PipelineOptions {

        /** Get the Google Cloud project id. */
        @Description("Google Cloud project id")
        @Validation.Required
        String getSpannerProjectId();

        void setSpannerProjectId(String value);

        /** Get the Spanner instance id to read data from. */
        @Description("The Spanner instance id to write into")
        @Validation.Required
        String getInputSpannerInstanceId();

        void setInputSpannerInstanceId(String value);

        /** Get the Spanner database name to read from. */
        @Description("Name of the Spanner database to read from")
        @Validation.Required
        String getInputSpannerDatabaseId();

        void setInputSpannerDatabaseId(String value);

        /** Get the Google Cloud project id. */
        @Description("Google Cloud project id")
        @Validation.Required
        String getBigQueryProjectId();

        void setBigQueryProjectId(String value);
    }

    public static void main(String[] args) {
        SpannerToBigQueryOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(SpannerToBigQueryOptions.class);

        final SpannerConfig spannerConfig =
                SpannerConfig.create()
                        .withProjectId(options.getSpannerProjectId())
                        .withInstanceId(options.getInputSpannerInstanceId())
                        .withDatabaseId(options.getInputSpannerDatabaseId());

        Pipeline p = Pipeline.create(options);

//        final PCollectionView<Transaction> transaction =
//                p.apply(
//                        SpannerIO.createTransaction()
//                                .withSpannerConfig(spannerConfig)
//                                .withTimestampBound(TimestampBound.ofReadTimestamp(Timestamp.now())));

//        final PCollection<Struct> rows =
//                p.apply(
//                    SpannerIO.read()
//                        .withSpannerConfig(spannerConfig)
//                        .withQuery(" SELECT"
//                                + "  t.table_catalog,"
//                                + "  t.table_schema,"
//                                + "  t.table_name,"
//                                + "  t.column_name,"
//                                + "  t.ordinal_position,"
//                                + "  t.column_default,"
//                                + "  t.data_type,"
//                                + "  t.is_nullable,"
//                                + "  t.spanner_type"
//                                + " FROM"
//                                + "  information_schema.columns AS t"
//                                + " ORDER BY"
//                                + "  t.table_catalog,"
//                                + "  t.table_schema,"
//                                + "  t.table_name,"
//                                + "  t.ordinal_position"));

//        final PCollection<Struct> rows =
//                p.apply(
//                        SpannerIO.read()
//                                .withSpannerConfig(spannerConfig)
//                                .withQuery("SELECT * FROM Albums LIMIT 100"));

//        // [START spanner_dataflow_readall]
//        PCollection<Struct> allRecords =
//                p.apply(SpannerIO.read()
//                    .withSpannerConfig(spannerConfig)
//                    .withQuery("SELECT t.table_name FROM information_schema.tables AS t WHERE t"
//                        + ".table_catalog = '' AND t.table_schema = ''"))
//                .apply(MapElements.into(TypeDescriptor.of(ReadOperation.class))
//                        .via((SerializableFunction<Struct, ReadOperation>) input -> {
//                            String tableName = input.getString(0);
//                            logger.info("tableName:" + tableName);
//                            return ReadOperation.create().withQuery("SELECT * FROM " + tableName);
//                        }))
//                .apply(SpannerIO.readAll().withSpannerConfig(spannerConfig));
//        // [END spanner_dataflow_readall]

        TableReference tableRef = new TableReference();
        tableRef.setProjectId(options.getBigQueryProjectId());
        tableRef.setDatasetId("spanner");
        tableRef.setTableId("Sample");

        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("Id").setType("STRING"));
        TableSchema schema = new TableSchema().setFields(fields);

        PCollection<Struct> records = p.apply(
                SpannerIO.read()
                        .withSpannerConfig(spannerConfig)
                        .withQuery("SELECT * FROM Albums"));

        records.apply(new StructToTableRowTransform())
                .apply(BigQueryIO.writeTableRows()
                        .withSchema(schema)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .to(tableRef));

        p.run().waitUntilFinish();
    }
}
