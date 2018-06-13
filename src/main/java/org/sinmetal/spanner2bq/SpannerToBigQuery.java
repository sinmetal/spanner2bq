package org.sinmetal.spanner2bq;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.spanner.Struct;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class SpannerToBigQuery {

    static final Logger logger = Logger.getLogger(SpannerToBigQuery.class.getSimpleName());

    public interface SpannerToBigQueryOptions extends PipelineOptions {

        /**
         * Get the Google Cloud project id.
         */
        @Description("Google Cloud project id")
        @Validation.Required
        String getSpannerProjectId();

        void setSpannerProjectId(String value);

        /**
         * Get the Spanner instance id to read data from.
         */
        @Description("The Spanner instance id to write into")
        @Validation.Required
        String getInputSpannerInstanceId();

        void setInputSpannerInstanceId(String value);

        /**
         * Get the Spanner database name to read from.
         */
        @Description("Name of the Spanner database to read from")
        @Validation.Required
        String getInputSpannerDatabaseId();

        void setInputSpannerDatabaseId(String value);

        /**
         * Get the Google Cloud project id.
         */
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

        Map<String, List<TableFieldSchema>> allTableSchema = SpannerUtil.getAllTableSchema(options.getSpannerProjectId(), options.getInputSpannerInstanceId(), options.getInputSpannerDatabaseId());

        for(Map.Entry<String, List<TableFieldSchema>> entry : allTableSchema.entrySet()) {
            String tableName = entry.getKey();

            TableReference tableRef = new TableReference();
            tableRef.setProjectId(options.getBigQueryProjectId());
            tableRef.setDatasetId("spanner");
            tableRef.setTableId(tableName);

            TableSchema schema = new TableSchema().setFields(entry.getValue());

            PCollection<Struct> records = p.apply(
                    tableName,
                    SpannerIO.read()
                            .withSpannerConfig(spannerConfig)
                            .withQuery("SELECT * FROM " + tableName));

            PCollection<TableRow> tableRows = records.apply(new StructToTableRowTransform());
            tableRows.apply(BigQueryIO.writeTableRows()
                    .withSchema(schema)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .to(tableRef));
        }

        p.run();
    }
}
