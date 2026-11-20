/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils;

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset.BinlogOffset;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.jdbc.JdbcConnection;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

public class MySqlConnectionUtilsTest {

    /**
     * Status statements MySqlConnection can pick. SHOW MASTER STATUS is the fallback used by
     * OceanBase, MySQL before 8.2 and MariaDB releases without the SHOW BINARY LOG STATUS alias
     * (MDEV-39519). SHOW BINARY LOG STATUS is used by MySQL 8.2 and later and by MariaDB releases
     * with that alias.
     */
    private static final String[] STATUS_STATEMENTS = {
        "SHOW MASTER STATUS", "SHOW BINARY LOG STATUS"
    };

    @Test
    void earliestBinlogOffsetListsFilesWithShowBinaryLogs() throws Exception {
        // Issue 13371: OceanBase rejects SHOW MASTER LOGS but accepts SHOW BINARY LOGS, which
        // every supported MySQL version also accepts. The listing statement must not follow the
        // status statement.
        for (String statusStatement : STATUS_STATEMENTS) {
            List<String> executed = new ArrayList<>();
            MySqlConnection connection = connection(statusStatement);
            Mockito.doAnswer(
                            invocation -> {
                                executed.add(invocation.getArgument(0));
                                ResultSet rs = Mockito.mock(ResultSet.class);
                                Mockito.when(rs.next()).thenReturn(true);
                                Mockito.when(rs.getString(1)).thenReturn("mysql-bin.000003");
                                JdbcConnection.ResultSetMapper<?> mapper =
                                        invocation.getArgument(1);
                                return mapper.apply(rs);
                            })
                    .when(connection)
                    .queryAndMap(anyString(), any(JdbcConnection.ResultSetMapper.class));

            BinlogOffset offset = MySqlConnectionUtils.earliestBinlogOffset(connection);

            Assertions.assertEquals(
                    "[SHOW BINARY LOGS]", executed.toString(), "status: " + statusStatement);
            Assertions.assertEquals("mysql-bin.000003", offset.getFilename());
            Assertions.assertEquals(4L, offset.getPosition());
        }
    }

    @Test
    void timestampLookupListsFilesWithShowBinaryLogs() throws Exception {
        // Issue 13371: the timestamp startup mode failed on OceanBase because this lookup ran
        // SHOW MASTER LOGS whenever the status statement was SHOW MASTER STATUS.
        for (String statusStatement : STATUS_STATEMENTS) {
            List<String> executed = new ArrayList<>();
            MySqlConnection connection = connection(statusStatement);
            Mockito.doAnswer(
                            invocation -> {
                                executed.add(invocation.getArgument(0));
                                ResultSet rs = Mockito.mock(ResultSet.class);
                                Mockito.when(rs.next()).thenReturn(false);
                                JdbcConnection.ResultSetConsumer consumer =
                                        invocation.getArgument(1);
                                consumer.accept(rs);
                                return connection;
                            })
                    .when(connection)
                    .query(anyString(), any(JdbcConnection.ResultSetConsumer.class));

            BinlogOffset offset =
                    MySqlConnectionUtils.findBinlogOffsetBytimestamp(
                            connection, Mockito.mock(BinaryLogClient.class), 1795143600000L);

            Assertions.assertEquals(
                    "[SHOW BINARY LOGS]", executed.toString(), "status: " + statusStatement);
            Assertions.assertEquals(BinlogOffset.INITIAL_OFFSET, offset);
        }
    }

    private static MySqlConnection connection(String statusStatement) {
        MySqlConnection connection = Mockito.mock(MySqlConnection.class);
        Mockito.when(connection.binaryLogStatusStatement()).thenReturn(statusStatement);
        return connection;
    }
}
