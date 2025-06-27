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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.security.authentication.HadoopAuthenticator;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataType;
import org.apache.doris.thrift.TPaimonMetadataParams;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Catalog.TableNotExistException;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowType;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class PaimonTableValuedFunction extends MetadataTableValuedFunction {

    public static final String NAME = "paimon_meta";
    public static final String TABLE = "table";
    public static final String QUERY_TYPE = "query_type";

    private static final ImmutableSet<String> PROPERTIES_SET = ImmutableSet.of(TABLE, QUERY_TYPE);

    private final String queryType;
    private final Table paimonSysTable;
    private final List<Column> schema;
    private final Map<String, String> hadoopProps;
    private final Map<String, String> paimonProps;
    private final HadoopAuthenticator hadoopAuthenticator;


    public static PaimonTableValuedFunction create(Map<String, String> params)
            throws AnalysisException {
        Map<String, String> validParams = Maps.newHashMap();
        for (String key : params.keySet()) {
            if (!PROPERTIES_SET.contains(key.toLowerCase())) {
                throw new AnalysisException("'" + key + "' is invalid property");
            }
            // check ctl, db, tbl
            validParams.put(key.toLowerCase(), params.get(key));
        }
        String tableName = validParams.get(TABLE);
        String queryType = validParams.get(QUERY_TYPE);
        if (tableName == null || queryType == null) {
            throw new AnalysisException("Invalid paimon metadata query");
        }

        if (queryType.equalsIgnoreCase("binlog") || queryType.equalsIgnoreCase("audit_log")) {
            throw new AnalysisException("SysTable " + queryType + " is not supported yet");
        }
        String[] names = tableName.split("\\.");
        if (names.length != 3) {
            throw new AnalysisException("The paimon table name contains the catalogName, databaseName, and tableName");
        }
        TableName paimonTableName = new TableName(names[0], names[1], names[2]);
        // check auth
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), paimonTableName, PrivPredicate.SELECT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SELECT",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    paimonTableName.getDb() + ": " + paimonTableName.getTbl());
        }
        return new PaimonTableValuedFunction(paimonTableName, queryType);
    }


    public PaimonTableValuedFunction(TableName paimonTableName, String queryType)
            throws AnalysisException {
        this.queryType = queryType;
        CatalogIf<?> dorsiCatalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(paimonTableName.getCtl());
        if (!(dorsiCatalog instanceof ExternalCatalog)) {
            throw new AnalysisException("Catalog " + paimonTableName.getCtl() + " is not an external catalog");
        }

        if (!(dorsiCatalog instanceof PaimonExternalCatalog)) {
            throw new AnalysisException("Catalog " + paimonTableName.getCtl() + " is not an paimon catalog");
        }

        PaimonExternalCatalog paimonExternalCatalog = (PaimonExternalCatalog) dorsiCatalog;
        hadoopProps = paimonExternalCatalog.getCatalogProperty().getHadoopProperties();
        paimonProps = paimonExternalCatalog.getPaimonOptionsMap();
        hadoopAuthenticator = paimonExternalCatalog.getPreExecutionAuthenticator().getHadoopAuthenticator();

        Table paimonTable;
        try {
            paimonTable = hadoopAuthenticator.doAs(() ->
                    PaimonUtil.getPaimonTable(paimonExternalCatalog, paimonTableName.getDb(),
                            paimonTableName.getTbl()));
        } catch (Exception e) {
            throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e));
        }

        if (paimonTable == null) {
            throw new AnalysisException("Paimon table " + paimonTableName + " does not exist");
        }

        // identifier.
        Identifier identifier = new Identifier(paimonTableName.getDb(), paimonTableName.getTbl(), null, queryType);
        Catalog catalog = paimonExternalCatalog.getCatalog();
        try {
            paimonSysTable = catalog.getTable(identifier);
        } catch (TableNotExistException e) {
            throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e));
        }

        if (paimonSysTable == null) {
            throw new AnalysisException("Unrecognized queryType for iceberg metadata:" + queryType);
        }

        // obtain all schema
        RowType rowType = paimonSysTable.rowType();
        this.schema = PaimonUtil.parseSchema(rowType);

    }

    @Override
    public TMetadataType getMetadataType() {
        return TMetadataType.PAIMON;
    }

    @Override
    public List<TMetaScanRange> getMetaScanRanges(List<String> requiredFileds) {
        List<TMetaScanRange> scanRanges = Lists.newArrayList();
        int[] projections = IntStream.range(0, requiredFileds.size())
                .toArray();
        List<Split> splits;

        try {
            splits = hadoopAuthenticator.doAs(() -> paimonSysTable.newReadBuilder()
                    .withProjection(projections).newScan().plan().splits());
        } catch (Exception e) {
            throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e));
        }
        //
        for (Split split : splits) {
            TMetaScanRange tMetaScanRange = new TMetaScanRange();
            tMetaScanRange.setMetadataType(TMetadataType.PAIMON);
            // set paimon metadata params
            TPaimonMetadataParams tPaimonMetadataParams = new TPaimonMetadataParams();
            tPaimonMetadataParams.setHadoopProps(hadoopProps);
            tPaimonMetadataParams.setPaimonProps(paimonProps);
            tPaimonMetadataParams.setSerializedTask(PaimonUtil.serialize(split));
            scanRanges.add(tMetaScanRange);
        }

        return scanRanges;
    }

    @Override
    public String getTableName() {
        return "PaimonTableValuedFunction<" + queryType + ">";
    }

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        return schema;
    }
}
