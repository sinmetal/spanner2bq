package org.sinmetal.spanner2bq;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.spanner.Struct;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class StructToTableRowTransform extends PTransform<PCollection<Struct>, PCollection<TableRow>> {

    @Override
    public PCollection<TableRow> expand(PCollection<Struct> input) {
        return input.apply(ParDo.of(new StructToTableRowFn()));
    }
}
