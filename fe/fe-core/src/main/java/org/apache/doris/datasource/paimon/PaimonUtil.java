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
import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.analysis.TableSnapshot.VersionType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HiveUtil;
import org.apache.doris.datasource.paimon.source.PaimonSource;
import org.apache.doris.thrift.TColumnType;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.schema.external.TArrayField;
import org.apache.doris.thrift.schema.external.TField;
import org.apache.doris.thrift.schema.external.TFieldPtr;
import org.apache.doris.thrift.schema.external.TMapField;
import org.apache.doris.thrift.schema.external.TNestedField;
import org.apache.doris.thrift.schema.external.TSchema;
import org.apache.doris.thrift.schema.external.TStructField;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.StartupMode;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.system.ReadOptimizedTable;
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
import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class PaimonUtil {
    private static final Logger LOG = LogManager.getLogger(PaimonUtil.class);
    private static final Base64.Encoder BASE64_ENCODER = java.util.Base64.getUrlEncoder().withoutPadding();
    private static final Pattern DIGITAL_REGEX = Pattern.compile("\\d+");

    private static final List<ConfigOption<?>>
            PAIMON_FROM_TIMESTAMP_CONFLICT_OPTIONS = Arrays.asList(
            CoreOptions.SCAN_SNAPSHOT_ID,
            CoreOptions.SCAN_TAG_NAME,
            CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS,
            CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP,
            CoreOptions.INCREMENTAL_BETWEEN,
            CoreOptions.INCREMENTAL_TO_AUTO_TAG);

    private static final List<ConfigOption<?>> PAIMON_FROM_SNAPSHOT_CONFLICT_OPTIONS = Arrays.asList(
            CoreOptions.SCAN_TIMESTAMP_MILLIS,
            CoreOptions.SCAN_TIMESTAMP,
            CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS,
            CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP,
            CoreOptions.INCREMENTAL_BETWEEN,
            CoreOptions.INCREMENTAL_TO_AUTO_TAG);

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

    /**
     * Builds a READ OPTIMIZED system table .
     *
     * <p>Read-optimized tables scan only the topmost level files that don't require merging,
     * which significantly improves reading performance for primary-key tables by avoiding
     * the LSM merge process. This is equivalent to accessing the '$ro' system table in Paimon.
     *
     * <p>Usage examples:
     * - Table hint: {@code @ro()}
     *
     * @param source the Paimon source containing catalog and table information
     * @return a read-optimized Table instance for faster querying
     */
    public static Table buildReadOptimizedTable(PaimonSource source) {
        PaimonExternalCatalog catalog = (PaimonExternalCatalog) source.getCatalog();
        ExternalTable externalTable = (ExternalTable) source.getTargetTable();
        return catalog.getPaimonSystemTable(externalTable.getOrBuildNameMapping(),
                ReadOptimizedTable.READ_OPTIMIZED);
    }

    /**
     * Builds a branch-specific table for time travel queries to a specific branch.
     *
     * <p>This method creates a table that reads from a specified branch, allowing users
     * to query data from different development or experimental branches without affecting
     * the main branch workflow.
     *
     * <p>Supported syntax:
     * - Table hint: {@code @branch(branch_name)} or {@code @branch("name"="branch_name")}
     * - SQL time travel: {@code FOR VERSION AS OF 'branch_name'}
     *
     * @param source the Paimon source containing catalog and table information
     * @param scanParams the scan parameters containing branch name information
     * @return a Table instance configured to read from the specified branch
     * @throws IllegalArgumentException if branch name is not properly specified in scanParams
     */
    public static Table buildBranchTable(PaimonSource source, TableScanParams scanParams) {
        String refName = extractRefName(scanParams);
        PaimonExternalCatalog catalog = (PaimonExternalCatalog) source.getCatalog();
        ExternalTable externalTable = (ExternalTable) source.getTargetTable();
        return catalog.getPaimonSystemTable(externalTable.getOrBuildNameMapping(), refName, null);
    }

    /**
     * Builds a tag-specific table for time travel queries to a specific tag.
     *
     * <p>Tags in Paimon represent immutable snapshots that preserve historical data
     * beyond the normal snapshot retention period. This method creates a table
     * configured to read data from a specific tag.
     *
     * <p>Supported syntax:
     * - Table hint: {@code @tag(tag_name)} or {@code @tag("name"="tag_name")}
     * - SQL time travel: {@code FOR VERSION AS OF 'tag_name'}
     *
     * <p>Note: This method clears conflicting scan options to ensure compatibility
     * with Paimon's FROM_SNAPSHOT startup mode requirements.
     *
     * @param baseTable the base Paimon table to copy configuration from
     * @param scanParams the scan parameters containing tag name information
     * @return a Table instance configured to read from the specified tag
     * @throws IllegalArgumentException if tag name is not properly specified in scanParams
     */
    public static Table buildTagTable(Table baseTable, TableScanParams scanParams) {
        String refName = extractRefName(scanParams);
        Map<String, String> options = new HashMap<>(baseTable.options());

        // For Paimon FROM_SNAPSHOT startup mode, must set only one key in:
        // [scan_tag_name, scan_watermark, scan_snapshot_id]
        options.put(CoreOptions.SCAN_TAG_NAME.key(), refName);
        options.put(CoreOptions.SCAN_WATERMARK.key(), null);
        options.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), null);
        options.putAll(excludePaimonConflictOptions(PAIMON_FROM_SNAPSHOT_CONFLICT_OPTIONS));

        return baseTable.copy(options);
    }

    /**
     * Builds a snapshot-specific table for time travel queries to a specific point in time.
     *
     * <p>This method supports both snapshot ID-based and timestamp-based time travel:
     * - Snapshot ID: queries a specific numbered snapshot
     * - Timestamp: queries the snapshot closest to the specified time
     *
     * <p>Supported syntax:
     * - SQL time travel by ID: {@code FOR VERSION AS OF 1}
     * - SQL time travel by timestamp: {@code FOR TIME AS OF '2023-01-01 12:00:00.001'}
     *
     * @param baseTable the base Paimon table to copy configuration from
     * @param tableSnapshot the snapshot specification (ID or timestamp)
     * @return a Table instance configured to read from the specified snapshot or timestamp
     * @throws DateTimeException if timestamp string cannot be parsed
     */
    public static Table buildSnapshotTable(Table baseTable, TableSnapshot tableSnapshot) {
        String value = tableSnapshot.getValue();
        TableSnapshot.VersionType type = tableSnapshot.getType();

        if (type == TableSnapshot.VersionType.VERSION && DIGITAL_REGEX.matcher(value).matches()) {
            return buildSnapshotIdTable(baseTable, value);
        } else if (type == VersionType.TIME && DIGITAL_REGEX.matcher(value).matches()) {
            return buildSnapshotTimestampMillisTable(baseTable, value);
        } else {
            return buildSnapshotTimestampTable(baseTable, value);
        }
    }

    /**
     * Builds a table configured to read from a specific snapshot ID.
     *
     * <p>Snapshot IDs are sequential numbers assigned to each commit in Paimon.
     * This method configures the table to read the exact state at the specified snapshot.
     *
     * <p>Note: Clears conflicting scan options to ensure compatibility with
     * Paimon's FROM_SNAPSHOT startup mode requirements.
     *
     * @param baseTable the base Paimon table to copy configuration from
     * @param snapshotId the snapshot ID as a string (e.g., "123")
     * @return a Table instance configured to read from the specified snapshot ID
     */
    public static Table buildSnapshotIdTable(Table baseTable, String snapshotId) {
        Map<String, String> options = new HashMap<>(baseTable.options());

        // For Paimon FROM_SNAPSHOT startup mode, must set only one key in:
        // [scan_tag_name, scan_watermark, scan_snapshot_id]
        options.put(CoreOptions.SCAN_TAG_NAME.key(), null);
        options.put(CoreOptions.SCAN_WATERMARK.key(), null);
        options.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), snapshotId);
        options.putAll(excludePaimonConflictOptions(PAIMON_FROM_SNAPSHOT_CONFLICT_OPTIONS));

        return baseTable.copy(options);
    }

    /**
     * Builds a table configured to read from a specific timestamp.
     *
     * <p>This method converts a timestamp string to epoch milliseconds and configures
     * the table to read the snapshot that was active at that point in time.
     *
     * <p>Supported timestamp formats depend on TimeUtils.timeStringToLong() implementation,
     * typically including ISO format and epoch timestamps.
     *
     * <p>Note: Clears conflicting scan options to ensure compatibility with
     * Paimon's FROM_TIMESTAMP startup mode requirements.
     *
     * @param baseTable the base Paimon table to copy configuration from
     * @param timestampStr the timestamp as a string
     * @return a Table instance configured to read from the specified timestamp
     * @throws DateTimeException if the timestamp string cannot be parsed
     */
    public static Table buildSnapshotTimestampTable(Table baseTable, String timestampStr) {
        Map<String, String> options = new HashMap<>(baseTable.options());

        // For Paimon FROM_TIMESTAMP startup mode, must set only one key in:
        // [scan_timestamp, scan_timestamp_millis]
        options.put(CoreOptions.SCAN_MODE.key(), "from-timestamp");
        options.put(CoreOptions.SCAN_TIMESTAMP.key(), timestampStr);
        options.put(CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), null);
        options.putAll(excludePaimonConflictOptions(PAIMON_FROM_TIMESTAMP_CONFLICT_OPTIONS));

        return baseTable.copy(options);
    }

    public static Table buildSnapshotTimestampMillisTable(Table baseTable, String timestampStr) {
        Map<String, String> options = new HashMap<>(baseTable.options());

        // For Paimon FROM_TIMESTAMP startup mode, must set only one key in:
        // [scan_timestamp, scan_timestamp_millis]
        options.put(CoreOptions.SCAN_MODE.key(), "from-timestamp");
        options.put(CoreOptions.SCAN_TIMESTAMP.key(), null);
        options.put(CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), timestampStr);
        options.putAll(excludePaimonConflictOptions(PAIMON_FROM_TIMESTAMP_CONFLICT_OPTIONS));

        return baseTable.copy(options);
    }

    /**
     * Extracts the reference name (branch or tag name) from table scan parameters.
     *
     * <p>This method supports two parameter formats:
     * 1. Map format: {@code {"name": "reference_name"}} - used in structured hints
     * 2. List format: {@code ["reference_name"]} - used in simple positional hints
     *
     * <p>Examples:
     * - Map format: {@code @branch("name"="main")} → "main"
     * - List format: {@code @tag(daily_backup)} → "daily_backup"
     *
     * @param scanParams the scan parameters containing reference name information
     * @return the extracted reference name
     * @throws IllegalArgumentException if the reference name is not properly specified
     *         or if the required "name" key is missing in map format
     */
    public static String extractRefName(TableScanParams scanParams) {
        if (!scanParams.getMapParams().isEmpty()) {
            if (!scanParams.getMapParams().containsKey("name")) {
                throw new IllegalArgumentException("must contain key 'name' in params");
            }
            return scanParams.getMapParams().get("name");
        } else {
            if (scanParams.getListParams().isEmpty() || scanParams.getListParams().get(0) == null) {
                throw new IllegalArgumentException("must contain a branch/tag name in params");
            }
            return scanParams.getListParams().get(0);
        }
    }

    /**
     * Creates a map of conflicting Paimon options with null values for exclusion.
     *
     * <p>This utility method helps ensure that conflicting scan options are properly
     * cleared when setting up specific scan modes. Paimon requires that only compatible
     * options are set for each startup mode to avoid configuration conflicts.
     *
     * <p>The returned map contains all specified illegal options as keys with null values,
     * which when applied to a table's options map, effectively removes those conflicting
     * configurations.
     *
     * @param illegalOptions the list of ConfigOptions that should be set to null
     * @return a HashMap containing the illegal options as keys with null values
     */
    public static Map<String, String> excludePaimonConflictOptions(List<ConfigOption<?>> illegalOptions) {
        return illegalOptions.stream()
                .collect(HashMap::new,
                        (m, option) -> m.put(option.key(), null),
                        HashMap::putAll);
    }
}
