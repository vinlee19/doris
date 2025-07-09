// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.datasource.paimon;

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.hive.HiveUtil;
import org.apache.doris.thrift.TColumnType;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.schema.external.TArrayField;
import org.apache.doris.thrift.schema.external.TField;
import org.apache.doris.thrift.schema.external.TFieldPtr;
import org.apache.doris.thrift.schema.external.TMapField;
import org.apache.doris.thrift.schema.external.TNestedField;
import org.apache.doris.thrift.schema.external.TSchema;
import org.apache.doris.thrift.schema.external.TStructField;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Projection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class PaimonUtil {
    private static final Logger LOG = LogManager.getLogger(PaimonUtil.class);
    private static final Base64.Encoder BASE64_ENCODER = java.util.Base64.getUrlEncoder().withoutPadding();

    // The keys of incremental read params for Paimon SDK
    private static final String PAIMON_SCAN_SNAPSHOT_ID = "scan.snapshot-id";
    private static final String PAIMON_SCAN_MODE = "scan.mode";
    private static final String PAIMON_INCREMENTAL_BETWEEN = "incremental-between";
    private static final String PAIMON_INCREMENTAL_BETWEEN_SCAN_MODE = "incremental-between-scan-mode";
    private static final String PAIMON_INCREMENTAL_BETWEEN_TIMESTAMP = "incremental-between-timestamp";

    // The keys of incremental read params for Doris Statement
    private static final String DORIS_START_SNAPSHOT_ID = "startSnapshotId";
    private static final String DORIS_END_SNAPSHOT_ID = "endSnapshotId";
    private static final String DORIS_START_TIMESTAMP = "startTimestamp";
    private static final String DORIS_END_TIMESTAMP = "endTimestamp";
    private static final String DORIS_INCREMENTAL_BETWEEN_SCAN_MODE = "incrementalBetweenScanMode";
    private static final String DEFAULT_INCREMENTAL_BETWEEN_SCAN_MODE = "auto";


    public static List<InternalRow> read(
            Table table, @Nullable int[] projection, @Nullable Predicate predicate,
            Pair<ConfigOption<?>, String>... dynamicOptions)
            throws IOException {
        Map<String, String> options = new HashMap<>();
        for (Pair<ConfigOption<?>, String> pair : dynamicOptions) {
            options.put(pair.getKey().key(), pair.getValue());
        }
        if (!options.isEmpty()) {
            table = table.copy(options);
        }
        ReadBuilder readBuilder = table.newReadBuilder();
        if (projection != null) {
            readBuilder.withProjection(projection);
        }
        if (predicate != null) {
            readBuilder.withFilter(predicate);
        }
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        InternalRowSerializer serializer =
                new InternalRowSerializer(
                        projection == null
                                ? table.rowType()
                                : Projection.of(projection).project(table.rowType()));
        List<InternalRow> rows = new ArrayList<>();
        reader.forEachRemaining(row -> rows.add(serializer.copy(row)));
        return rows;
    }

    public static PaimonPartitionInfo generatePartitionInfo(List<Column> partitionColumns,
            List<Partition> paimonPartitions) {

        if (CollectionUtils.isEmpty(partitionColumns) || paimonPartitions.isEmpty()) {
            return PaimonPartitionInfo.EMPTY;
        }

        Map<String, PartitionItem> nameToPartitionItem = Maps.newHashMap();
        Map<String, Partition> nameToPartition = Maps.newHashMap();
        PaimonPartitionInfo partitionInfo = new PaimonPartitionInfo(nameToPartitionItem, nameToPartition);

        for (Partition partition : paimonPartitions) {
            Map<String, String> spec = partition.spec();
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> entry : spec.entrySet()) {
                sb.append(entry.getKey()).append("=").append(entry.getValue()).append("/");
            }
            if (sb.length() > 0) {
                sb.deleteCharAt(sb.length() - 1);
            }
            String partitionName = sb.toString();
            nameToPartition.put(partitionName, partition);
            try {
                // partition values return by paimon api, may have problem,
                // to avoid affecting the query, we catch exceptions here
                nameToPartitionItem.put(partitionName, toListPartitionItem(partitionName, partitionColumns));
            } catch (Exception e) {
                LOG.warn("toListPartitionItem failed, partitionColumns: {}, partitionValues: {}",
                        partitionColumns, partition.spec(), e);
            }
        }
        return partitionInfo;
    }

    public static ListPartitionItem toListPartitionItem(String partitionName, List<Column> partitionColumns)
            throws AnalysisException {
        List<Type> types = partitionColumns.stream()
                .map(Column::getType)
                .collect(Collectors.toList());
        // Partition name will be in format: nation=cn/city=beijing
        // parse it to get values "cn" and "beijing"
        List<String> partitionValues = HiveUtil.toPartitionValues(partitionName);
        Preconditions.checkState(partitionValues.size() == types.size(), partitionName + " vs. " + types);
        List<PartitionValue> values = Lists.newArrayListWithExpectedSize(types.size());
        for (String partitionValue : partitionValues) {
            // null  will in partition 'null'
            // "null" will in partition 'null'
            // NULL  will in partition 'null'
            // "NULL" will in partition 'NULL'
            // values.add(new PartitionValue(partitionValue, "null".equals(partitionValue)));
            values.add(new PartitionValue(partitionValue, false));
        }
        PartitionKey key = PartitionKey.createListPartitionKeyWithTypes(values, types, true);
        ListPartitionItem listPartitionItem = new ListPartitionItem(Lists.newArrayList(key));
        return listPartitionItem;
    }

    private static Type paimonPrimitiveTypeToDorisType(org.apache.paimon.types.DataType dataType) {
        int tsScale = 3; // default
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return Type.BOOLEAN;
            case INTEGER:
                return Type.INT;
            case BIGINT:
                return Type.BIGINT;
            case FLOAT:
                return Type.FLOAT;
            case DOUBLE:
                return Type.DOUBLE;
            case SMALLINT:
                return Type.SMALLINT;
            case TINYINT:
                return Type.TINYINT;
            case VARCHAR:
                return ScalarType.createVarcharType(((VarCharType) dataType).getLength());
            case CHAR:
                return ScalarType.createCharType(((CharType) dataType).getLength());
            case BINARY:
            case VARBINARY:
                return Type.STRING;
            case DECIMAL:
                DecimalType decimal = (DecimalType) dataType;
                return ScalarType.createDecimalV3Type(decimal.getPrecision(), decimal.getScale());
            case DATE:
                return ScalarType.createDateV2Type();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                if (dataType instanceof org.apache.paimon.types.TimestampType) {
                    tsScale = ((org.apache.paimon.types.TimestampType) dataType).getPrecision();
                    if (tsScale > 6) {
                        tsScale = 6;
                    }
                } else if (dataType instanceof org.apache.paimon.types.LocalZonedTimestampType) {
                    tsScale = ((org.apache.paimon.types.LocalZonedTimestampType) dataType).getPrecision();
                    if (tsScale > 6) {
                        tsScale = 6;
                    }
                }
                return ScalarType.createDatetimeV2Type(tsScale);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (dataType instanceof org.apache.paimon.types.LocalZonedTimestampType) {
                    tsScale = ((org.apache.paimon.types.LocalZonedTimestampType) dataType).getPrecision();
                    if (tsScale > 6) {
                        tsScale = 6;
                    }
                }
                return ScalarType.createDatetimeV2Type(tsScale);
            case ARRAY:
                ArrayType arrayType = (ArrayType) dataType;
                Type innerType = paimonPrimitiveTypeToDorisType(arrayType.getElementType());
                return org.apache.doris.catalog.ArrayType.create(innerType, true);
            case MAP:
                MapType mapType = (MapType) dataType;
                return new org.apache.doris.catalog.MapType(
                        paimonTypeToDorisType(mapType.getKeyType()), paimonTypeToDorisType(mapType.getValueType()));
            case ROW:
                RowType rowType = (RowType) dataType;
                List<DataField> fields = rowType.getFields();
                return new org.apache.doris.catalog.StructType(fields.stream()
                        .map(field -> new org.apache.doris.catalog.StructField(field.name(),
                                paimonTypeToDorisType(field.type())))
                        .collect(Collectors.toCollection(ArrayList::new)));
            case TIME_WITHOUT_TIME_ZONE:
                return Type.UNSUPPORTED;
            default:
                LOG.warn("Cannot transform unknown type: " + dataType.getTypeRoot());
                return Type.UNSUPPORTED;
        }
    }

    public static Type paimonTypeToDorisType(org.apache.paimon.types.DataType type) {
        return paimonPrimitiveTypeToDorisType(type);
    }

    public static void updatePaimonColumnUniqueId(Column column, DataType dataType) {
        List<Column> columns = column.getChildren();
        switch (dataType.getTypeRoot()) {
            case ARRAY:
                ArrayType arrayType = (ArrayType) dataType;
                updatePaimonColumnUniqueId(columns.get(0), arrayType.getElementType());
                break;
            case MAP:
                MapType mapType = (MapType) dataType;
                updatePaimonColumnUniqueId(columns.get(0), mapType.getKeyType());
                updatePaimonColumnUniqueId(columns.get(1), mapType.getValueType());
                break;
            case ROW:
                RowType rowType = (RowType) dataType;
                for (int idx = 0; idx < columns.size(); idx++) {
                    updatePaimonColumnUniqueId(columns.get(idx), rowType.getFields().get(idx));
                }
                break;
            default:
                return;
        }
    }

    public static void updatePaimonColumnUniqueId(Column column, DataField field) {
        column.setUniqueId(field.id());
        updatePaimonColumnUniqueId(column, field.type());
    }

    public static TField getSchemaInfo(DataType dataType) {
        TField field = new TField();
        field.setIsOptional(dataType.isNullable());
        TNestedField nestedField = new TNestedField();
        switch (dataType.getTypeRoot()) {
            case ARRAY: {
                TArrayField listField = new TArrayField();
                org.apache.paimon.types.ArrayType paimonArrayType = (org.apache.paimon.types.ArrayType) dataType;
                TFieldPtr fieldPtr = new TFieldPtr();
                fieldPtr.setFieldPtr(getSchemaInfo(paimonArrayType.getElementType()));
                listField.setItemField(fieldPtr);
                nestedField.setArrayField(listField);
                field.setNestedField(nestedField);

                TColumnType tColumnType = new TColumnType();
                tColumnType.setType(TPrimitiveType.ARRAY);
                field.setType(tColumnType);
                break;
            }
            case MAP: {
                TMapField mapField = new TMapField();
                org.apache.paimon.types.MapType mapType = (org.apache.paimon.types.MapType) dataType;
                TFieldPtr keyField = new TFieldPtr();
                keyField.setFieldPtr(getSchemaInfo(mapType.getKeyType()));
                mapField.setKeyField(keyField);
                TFieldPtr valueField = new TFieldPtr();
                valueField.setFieldPtr(getSchemaInfo(mapType.getValueType()));
                mapField.setValueField(valueField);
                nestedField.setMapField(mapField);
                field.setNestedField(nestedField);

                TColumnType tColumnType = new TColumnType();
                tColumnType.setType(TPrimitiveType.MAP);
                field.setType(tColumnType);
                break;
            }
            case ROW: {
                RowType rowType = (RowType) dataType;
                TStructField structField = getSchemaInfo(rowType.getFields());
                nestedField.setStructField(structField);
                field.setNestedField(nestedField);

                TColumnType tColumnType = new TColumnType();
                tColumnType.setType(TPrimitiveType.STRUCT);
                field.setType(tColumnType);
                break;
            }
            default:
                field.setType(paimonPrimitiveTypeToDorisType(dataType).toColumnTypeThrift());
                break;
        }
        return field;
    }

    public static TStructField getSchemaInfo(List<DataField> paimonFields) {
        TStructField structField = new TStructField();
        for (DataField paimonField : paimonFields) {
            TField childField = getSchemaInfo(paimonField.type());
            childField.setName(paimonField.name());
            childField.setId(paimonField.id());
            TFieldPtr fieldPtr = new TFieldPtr();
            fieldPtr.setFieldPtr(childField);
            structField.addToFields(fieldPtr);
        }
        return structField;
    }

    public static TSchema getSchemaInfo(TableSchema paimonTableSchema) {
        TSchema tSchema = new TSchema();
        tSchema.setSchemaId(paimonTableSchema.id());
        tSchema.setRootField(getSchemaInfo(paimonTableSchema.fields()));
        return tSchema;
    }

    public static List<Column> parseSchema(Table table) {
        List<String> primaryKeys = table.primaryKeys();
        return parseSchema(table.rowType(), primaryKeys);
    }

    public static List<Column> parseSchema(RowType rowType, List<String> primaryKeys) {
        List<Column> resSchema = Lists.newArrayListWithCapacity(rowType.getFields().size());
        rowType.getFields().forEach(field -> {
            resSchema.add(new Column(field.name().toLowerCase(),
                    PaimonUtil.paimonTypeToDorisType(field.type()),
                    primaryKeys.contains(field.name()),
                    null,
                    field.type().isNullable(),
                    field.description(),
                    true,
                    field.id()));
        });
        return resSchema;
    }

    public static <T> String encodeObjectToString(T t) {
        try {
            byte[] bytes = InstantiationUtil.serializeObject(t);
            return new String(BASE64_ENCODER.encode(bytes), java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @VisibleForTesting
    public static Map<String, String> validateIncrementalReadParams(Map<String, String> params) throws UserException {
        // Check if snapshot-based parameters exist
        boolean hasStartSnapshotId = params.containsKey(DORIS_START_SNAPSHOT_ID)
                && params.get(DORIS_START_SNAPSHOT_ID) != null;
        boolean hasEndSnapshotId = params.containsKey(DORIS_END_SNAPSHOT_ID)
                && params.get(DORIS_END_SNAPSHOT_ID) != null;
        boolean hasIncrementalBetweenScanMode = params.containsKey(DORIS_INCREMENTAL_BETWEEN_SCAN_MODE)
                && params.get(DORIS_INCREMENTAL_BETWEEN_SCAN_MODE) != null;

        // Check if timestamp-based parameters exist
        boolean hasStartTimestamp = params.containsKey(DORIS_START_TIMESTAMP)
                && params.get(DORIS_START_TIMESTAMP) != null;
        boolean hasEndTimestamp = params.containsKey(DORIS_END_TIMESTAMP) && params.get(DORIS_END_TIMESTAMP) != null;

        // Check if any snapshot-based parameters are present
        boolean hasSnapshotParams = hasStartSnapshotId || hasEndSnapshotId || hasIncrementalBetweenScanMode;

        // Check if any timestamp-based parameters are present
        boolean hasTimestampParams = hasStartTimestamp || hasEndTimestamp;

        // Rule 2: The two groups are mutually exclusive
        if (hasSnapshotParams && hasTimestampParams) {
            throw new UserException(
                    "Cannot specify both snapshot-based parameters"
                            + "(startSnapshotId, endSnapshotId, incrementalBetweenScanMode) "
                            + "and timestamp-based parameters (startTimestamp, endTimestamp) at the same time");
        }

        // Validate snapshot-based parameters group
        if (hasSnapshotParams) {
            // Rule 3.1 & 3.2: DORIS_START_SNAPSHOT_ID is required
            if (!hasStartSnapshotId) {
                throw new UserException("startSnapshotId is required when using snapshot-based incremental read");
            }

            // Rule 3.3: DORIS_INCREMENTAL_BETWEEN_SCAN_MODE can only appear
            // when both start and end snapshot IDs are specified
            if (hasIncrementalBetweenScanMode && (!hasStartSnapshotId || !hasEndSnapshotId)) {
                throw new UserException(
                        "incrementalBetweenScanMode can only be specified when"
                                + " both startSnapshotId and endSnapshotId are provided");
            }

            // Validate snapshot ID values
            if (hasStartSnapshotId) {
                try {
                    long startSId = Long.parseLong(params.get(DORIS_START_SNAPSHOT_ID));
                    if (startSId <= 0) {
                        throw new UserException("startSnapshotId must be greater than 0");
                    }
                } catch (NumberFormatException e) {
                    throw new UserException("Invalid startSnapshotId format: " + e.getMessage());
                }
            }

            if (hasEndSnapshotId) {
                try {
                    long endSId = Long.parseLong(params.get(DORIS_END_SNAPSHOT_ID));
                    if (endSId <= 0) {
                        throw new UserException("endSnapshotId must be greater than 0");
                    }
                } catch (NumberFormatException e) {
                    throw new UserException("Invalid endSnapshotId format: " + e.getMessage());
                }
            }

            // Check if both snapshot IDs are present and validate their relationship
            if (hasStartSnapshotId && hasEndSnapshotId) {
                try {
                    long startSId = Long.parseLong(params.get(DORIS_START_SNAPSHOT_ID));
                    long endSId = Long.parseLong(params.get(DORIS_END_SNAPSHOT_ID));
                    if (startSId >= endSId) {
                        throw new UserException("startSnapshotId must be less than endSnapshotId");
                    }
                } catch (NumberFormatException e) {
                    throw new UserException("Invalid snapshot ID format: " + e.getMessage());
                }
            }

            // Validate DORIS_INCREMENTAL_BETWEEN_SCAN_MODE
            if (hasIncrementalBetweenScanMode) {
                String scanMode = params.get(DORIS_INCREMENTAL_BETWEEN_SCAN_MODE).toLowerCase();
                if (!scanMode.equals("auto") && !scanMode.equals("diff")
                        && !scanMode.equals("delta") && !scanMode.equals("changelog")) {
                    throw new UserException("incrementalBetweenScanMode must be one of: auto, diff, delta, changelog");
                }
            }
        }

        // Validate timestamp-based parameters group
        if (hasTimestampParams) {
            // Rule 4.1 & 4.2: DORIS_START_TIMESTAMP is required
            if (!hasStartTimestamp) {
                throw new UserException("startTimestamp is required when using timestamp-based incremental read");
            }

            // Validate timestamp values
            if (hasStartTimestamp) {
                try {
                    long startTS = Long.parseLong(params.get(DORIS_START_TIMESTAMP));
                    if (startTS < 0) {
                        throw new UserException("startTimestamp must be greater than or equal to 0");
                    }
                } catch (NumberFormatException e) {
                    throw new UserException("Invalid startTimestamp format: " + e.getMessage());
                }
            }

            if (hasEndTimestamp) {
                try {
                    long endTS = Long.parseLong(params.get(DORIS_END_TIMESTAMP));
                    if (endTS <= 0) {
                        throw new UserException("endTimestamp must be greater than 0");
                    }
                } catch (NumberFormatException e) {
                    throw new UserException("Invalid endTimestamp format: " + e.getMessage());
                }
            }

            // Check if both timestamps are present and validate their relationship
            if (hasStartTimestamp && hasEndTimestamp) {
                try {
                    long startTS = Long.parseLong(params.get(DORIS_START_TIMESTAMP));
                    long endTS = Long.parseLong(params.get(DORIS_END_TIMESTAMP));
                    if (startTS >= endTS) {
                        throw new UserException("startTimestamp must be less than endTimestamp");
                    }
                } catch (NumberFormatException e) {
                    throw new UserException("Invalid timestamp format: " + e.getMessage());
                }
            }
        }

        // If no incremental parameters are provided at all, that's also invalid in this context
        if (!hasSnapshotParams && !hasTimestampParams) {
            throw new UserException(
                    "Invalid paimon incremental read params: at least one valid parameter group must be specified");
        }

        // Fill the result map based on parameter combinations
        Map<String, String> paimonScanParams = new HashMap<>();
        paimonScanParams.put(PAIMON_SCAN_SNAPSHOT_ID, null);
        paimonScanParams.put(PAIMON_SCAN_MODE, null);

        if (hasSnapshotParams) {
            paimonScanParams.put(PAIMON_SCAN_MODE, null);
            if (hasStartSnapshotId && !hasEndSnapshotId) {
                // Only startSnapshotId is specified
                paimonScanParams.put(PAIMON_SCAN_SNAPSHOT_ID, params.get(DORIS_START_SNAPSHOT_ID));
            } else if (hasStartSnapshotId && hasEndSnapshotId) {
                // Both start and end snapshot IDs are specified
                String startSId = params.get(DORIS_START_SNAPSHOT_ID);
                String endSId = params.get(DORIS_END_SNAPSHOT_ID);
                paimonScanParams.put(PAIMON_INCREMENTAL_BETWEEN, startSId + "," + endSId);
            }

            // Add incremental between scan mode if present
            if (hasIncrementalBetweenScanMode) {
                paimonScanParams.put(PAIMON_INCREMENTAL_BETWEEN_SCAN_MODE,
                        params.get(DORIS_INCREMENTAL_BETWEEN_SCAN_MODE));
            }
        }

        if (hasTimestampParams) {
            String startTS = params.get(DORIS_START_TIMESTAMP);
            String endTS = params.get(DORIS_END_TIMESTAMP);

            if (hasStartTimestamp && !hasEndTimestamp) {
                // Only startTimestamp is specified
                paimonScanParams.put(PAIMON_INCREMENTAL_BETWEEN_TIMESTAMP, startTS + "," + Long.MAX_VALUE);
            } else if (hasStartTimestamp && hasEndTimestamp) {
                // Both start and end timestamps are specified
                paimonScanParams.put(PAIMON_INCREMENTAL_BETWEEN_TIMESTAMP, startTS + "," + endTS);
            }
        }

        return paimonScanParams;
    }

}
