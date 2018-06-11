package org.sinmetal.spanner2bq;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.spanner.Struct;
import org.apache.beam.sdk.transforms.DoFn;

public class StructToTableRowFn extends DoFn<Struct, TableRow> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        TableRow row = new TableRow();
        row.put("Id", c.element().getString("Key"));
        c.output(row);
    }
}
