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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite ("k1ap2spa") {

    sql """ DROP TABLE IF EXISTS d_table; """

    sql """
            create table d_table(
                k1 int null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) null
            )
            duplicate key (k1,k2,k3)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into d_table select 1,1,1,'a';"
    sql "insert into d_table select 2,2,2,'b';"
    sql "insert into d_table select 3,-3,null,'c';"

    createMV("create materialized view k1ap2spa as select abs(k1)+1,sum(abs(k2+1)) from d_table group by abs(k1)+1;")

    sql "insert into d_table select -4,-4,-4,'d';"
    sql "insert into d_table(k4,k2) values('d',4);"

    qt_select_star "select * from d_table order by k1;"

    sql """alter table d_table modify column k1 set stats ('row_count'='5');"""

    sql "analyze table d_table with sync;"
    sql """set enable_stats=false;"""

    mv_rewrite_success("select abs(k1)+1 t,sum(abs(k2+1)) from d_table group by t order by t;", "k1ap2spa")
    qt_select_mv "select abs(k1)+1 t,sum(abs(k2+1)) from d_table group by t order by t;"

    sql """set enable_stats=true;"""

    mv_rewrite_success("select abs(k1)+1 t,sum(abs(k2+1)) from d_table group by t order by t;", "k1ap2spa")

}
