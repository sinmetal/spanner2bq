package org.sinmetal.spanner2bq;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import org.apache.beam.sdk.transforms.DoFn;

import java.text.SimpleDateFormat;
import java.util.Date;

public class StructToTableRowFn extends DoFn<Struct, TableRow> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        TableRow row = new TableRow();
        for(Type.StructField sf : c.element().getType().getStructFields()) {
            switch(sf.getType().getCode()) {
                case BOOL:
                    try {
                        row.put(sf.getName(), c.element().getBoolean(sf.getName()));
                    } catch (NullPointerException e) {
                        row.put(sf.getName(), null);
                    }
                    break;
                case INT64:
                    try {
                        row.put(sf.getName(), c.element().getLong(sf.getName()));
                    } catch (NullPointerException e) {
                        row.put(sf.getName(), null);
                    }
                    break;
                case FLOAT64:
                    try {
                        row.put(sf.getName(), c.element().getDouble(sf.getName()));
                    } catch (NullPointerException e) {
                        row.put(sf.getName(), null);
                    }
                    break;
                case STRING:
                    try {
                        row.put(sf.getName(), c.element().getString(sf.getName()));
                    } catch (NullPointerException e) {
                        row.put(sf.getName(), null);
                    }
                    break;
                case BYTES:
                    // BYTESはBigQueryに入れてもどうにもできないのでスルー
                    break;
                case TIMESTAMP:
                    try {
                        Timestamp timestamp = c.element().getTimestamp(sf.getName());
                        Date date = DateUtil.convertTimestampToDate(timestamp);
                        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
                        row.put(sf.getName(), fmt.format(date));
                    } catch (NullPointerException e) {
                        row.put(sf.getName(), null);
                    }
                    break;
                case DATE:
                    try {
                        com.google.cloud.Date org = c.element().getDate(sf.getName());
                        Date date = DateUtil.convertDateToDate(org);
                        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
                        row.put(sf.getName(), fmt.format(date));
                    } catch (NullPointerException e) {
                        row.put(sf.getName(), null);
                    }
                    break;
                case STRUCT:
                    break;
                case ARRAY:
                    switch(sf.getType().getArrayElementType().getCode()) {
                        case BOOL:
                            try {
                                row.put(sf.getName(), c.element().getBooleanList(sf.getName()));
                            } catch (NullPointerException e) {
                                row.put(sf.getName(), null);
                            }
                            break;
                        case INT64:
                            try {
                                row.put(sf.getName(), c.element().getLongList(sf.getName()));
                            } catch (NullPointerException e) {
                                row.put(sf.getName(), null);
                            }
                            break;
                        case FLOAT64:
                            try {
                                row.put(sf.getName(), c.element().getDoubleList(sf.getName()));
                            } catch (NullPointerException e) {
                                row.put(sf.getName(), null);
                            }
                            break;
                        case STRING:
                            try {
                                row.put(sf.getName(), c.element().getStringList(sf.getName()));
                            } catch (NullPointerException e) {
                                row.put(sf.getName(), null);
                            }
                            break;
                        case BYTES:
                            // BYTESはBigQueryに入れてもどうにもできないのでスルー
                            break;
                        case TIMESTAMP:
                            try {
                                row.put(sf.getName(), c.element().getTimestampList(sf.getName()));
                            } catch (NullPointerException e) {
                                row.put(sf.getName(), null);
                            }
                            break;
                        case DATE:
                            try {
                                row.put(sf.getName(), c.element().getDateList(sf.getName()));
                            } catch (NullPointerException e) {
                                row.put(sf.getName(), null);
                            }
                            break;
                        case STRUCT:
                            // TODO ListStructは難しいか？
                            try {
                                row.put(sf.getName(), c.element().getStructList(sf.getName()));
                            } catch (NullPointerException e) {
                                row.put(sf.getName(), null);
                            }
                            break;
                        default:
                            throw new AssertionError("Invalid ARRAY Internal type. code = " + sf.getType().getCode() + ", type = " + sf.getType());
                    }
                    break;
                default:
                    throw new AssertionError("Invalid type. code = " + sf.getType().getCode() + ", type = " + sf.getType());
            }
        }

        c.output(row);
    }
}
