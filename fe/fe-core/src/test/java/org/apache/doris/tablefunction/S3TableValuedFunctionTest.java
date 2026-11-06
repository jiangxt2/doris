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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.S3Util;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.functions.table.S3;
import org.apache.doris.proto.InternalService.PFetchTableSchemaRequest;
import org.apache.doris.proto.InternalService.PFetchTableSchemaResult;
import org.apache.doris.proto.Types.PScalarType;
import org.apache.doris.proto.Types.PStatus;
import org.apache.doris.proto.Types.PTypeDesc;
import org.apache.doris.proto.Types.PTypeNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTypeNodeType;

import org.apache.thrift.TDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class S3TableValuedFunctionTest {
    private static final Map<String, String> PROPERTIES = Map.of(
            "uri", "s3://bucket/logs/*", "format", "csv", "s3.endpoint", "s3.us-west-2.amazonaws.com",
            "s3.region", "us-west-2", "s3.access_key", "ak", "s3.secret_key", "sk");
    private boolean runningUnitTest;
    private ConnectContext previousContext;

    @BeforeEach
    public void setUp() {
        runningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        previousContext = ConnectContext.get();
        ConnectContext ctx = new ConnectContext();
        ctx.setStatementContext(new StatementContext());
        ctx.setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        FeConstants.runningUnitTest = runningUnitTest;
        ConnectContext.remove();
        if (previousContext != null) {
            previousContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testExactFilesKeepLiteralPathsAndSkipListing() throws Exception {
        FeConstants.runningUnitTest = false;
        TBrokerFileStatus file = file("a,b[1]*?.csv", 123);
        try (MockedStatic<S3Util> s3 = Mockito.mockStatic(S3Util.class);
                MockedStatic<FileSystemFactory> filesystems = Mockito.mockStatic(FileSystemFactory.class)) {
            S3TableValuedFunction tvf = new S3TableValuedFunction(PROPERTIES, Collections.singletonList(file));
            Assertions.assertEquals(Collections.singletonList(file), tvf.getFileStatuses());
            s3.verify(() -> S3Util.validateAndTestEndpoint("s3.us-west-2.amazonaws.com"));
            filesystems.verifyNoInteractions();
        }
    }

    @Test
    public void testSchemaFailurePreservesFilesAndAllowsRetry() throws Exception {
        assertSchemaFailure(TStatusCode.NOT_FOUND);
        assertSchemaFailure(TStatusCode.INTERNAL_ERROR);
    }

    @Test
    public void testSchemaInferenceUsesNextFileAfterNotFound() throws Exception {
        S3TableValuedFunction tvf = tvf();
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        PFetchTableSchemaResult schema = PFetchTableSchemaResult.newBuilder()
                .setStatus(PStatus.newBuilder().setStatusCode(TStatusCode.OK.getValue()))
                .setColumnNums(1).addColumnNames("id")
                .addColumnTypes(PTypeDesc.newBuilder().addTypes(PTypeNode.newBuilder()
                        .setType(TTypeNodeType.SCALAR.getValue())
                        .setScalarType(PScalarType.newBuilder().setType(TPrimitiveType.INT.getValue())))).build();
        Mockito.when(proxy.fetchTableStructureAsync(Mockito.any(), Mockito.any()))
                .thenReturn(CompletableFuture.completedFuture(result(TStatusCode.NOT_FOUND)),
                        CompletableFuture.completedFuture(schema));
        try (MockedStatic<BackendServiceProxy> service = Mockito.mockStatic(BackendServiceProxy.class)) {
            service.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            Assertions.assertEquals("id", tvf.getTableColumns().get(0).getName());
            Assertions.assertEquals(PrimitiveType.INT, tvf.getTableColumns().get(0).getType().getPrimitiveType());
            ArgumentCaptor<PFetchTableSchemaRequest> requests =
                    ArgumentCaptor.forClass(PFetchTableSchemaRequest.class);
            Mockito.verify(proxy, Mockito.times(2)).fetchTableStructureAsync(Mockito.any(), requests.capture());
            for (int i = 0; i < 2; i++) {
                TFileScanRange range = new TFileScanRange();
                new TDeserializer().deserialize(range, requests.getAllValues().get(i).getFileScanRange().toByteArray());
                Assertions.assertEquals(tvf.getFileStatuses().get(i).getPath(), range.getRanges().get(0).getPath());
            }
            Assertions.assertEquals(2, tvf.getFileStatuses().size());
        }
    }

    @Test
    public void testMergeSchemaReadsEveryFileAndFailsOnMissingFile() throws Exception {
        for (String format : Arrays.asList("parquet", "orc")) {
            Map<String, String> props = new HashMap<>(PROPERTIES);
            props.put("format", format);
            props.put("merge_schema", "true");
            S3TableValuedFunction tvf = Mockito.spy(new S3TableValuedFunction(props,
                    Arrays.asList(file("old", 10), file("empty", 0), file("new", 10))));
            Backend backend = Mockito.mock(Backend.class);
            Mockito.when(backend.getHost()).thenReturn("127.0.0.1");
            Mockito.when(backend.getBrpcPort()).thenReturn(8060);
            Mockito.doReturn(backend).when(tvf).getBackend();
            BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
            Mockito.when(proxy.fetchTableStructureAsync(Mockito.any(), Mockito.any()))
                    .thenReturn(CompletableFuture.completedFuture(schema("id", TPrimitiveType.INT)),
                            CompletableFuture.completedFuture(schema("campaign", TPrimitiveType.STRING)));
            try (MockedStatic<BackendServiceProxy> service = Mockito.mockStatic(BackendServiceProxy.class)) {
                service.when(BackendServiceProxy::getInstance).thenReturn(proxy);
                Assertions.assertEquals(Arrays.asList("campaign", "id"), tvf.getTableColumns().stream()
                        .map(Column::getName).collect(Collectors.toList()));
                Assertions.assertTrue(tvf.getTableColumns().stream().allMatch(Column::isAllowNull));
                ArgumentCaptor<PFetchTableSchemaRequest> requests =
                        ArgumentCaptor.forClass(PFetchTableSchemaRequest.class);
                Mockito.verify(proxy, Mockito.times(2)).fetchTableStructureAsync(Mockito.any(), requests.capture());
                for (int i = 0; i < 2; i++) {
                    TFileScanRange range = new TFileScanRange();
                    new TDeserializer().deserialize(range,
                            requests.getAllValues().get(i).getFileScanRange().toByteArray());
                    Assertions.assertEquals(tvf.getFileStatuses().get(i * 2).getPath(),
                            range.getRanges().get(0).getPath());
                }
            }
            S3TableValuedFunction failing = Mockito.spy(new S3TableValuedFunction(props,
                    Arrays.asList(file("old", 10), file("missing", 10))));
            Mockito.doReturn(backend).when(failing).getBackend();
            BackendServiceProxy failureProxy = Mockito.mock(BackendServiceProxy.class);
            Mockito.when(failureProxy.fetchTableStructureAsync(Mockito.any(), Mockito.any()))
                    .thenReturn(CompletableFuture.completedFuture(schema("id", TPrimitiveType.INT)),
                            CompletableFuture.completedFuture(result(TStatusCode.NOT_FOUND)));
            try (MockedStatic<BackendServiceProxy> service = Mockito.mockStatic(BackendServiceProxy.class)) {
                service.when(BackendServiceProxy::getInstance).thenReturn(failureProxy);
                Assertions.assertThrows(AnalysisException.class, failing::getTableColumns);
                Assertions.assertEquals(2, failing.getFileStatuses().size());
                Mockito.verify(failureProxy, Mockito.times(2)).fetchTableStructureAsync(Mockito.any(), Mockito.any());
            }
        }
    }

    @Test
    public void testMergeSchemaStopsSubmittingAfterCancellation() throws Exception {
        for (boolean cancelBeforeFirstFile : Arrays.asList(true, false)) {
            Map<String, String> props = new HashMap<>(PROPERTIES);
            props.put("format", "parquet");
            props.put("merge_schema", "true");
            S3TableValuedFunction tvf = Mockito.spy(new S3TableValuedFunction(props,
                    Arrays.asList(file("old", 10), file("new", 10))));
            Backend backend = Mockito.mock(Backend.class);
            Mockito.when(backend.getHost()).thenReturn("127.0.0.1");
            Mockito.when(backend.getBrpcPort()).thenReturn(8060);
            Mockito.doReturn(backend).when(tvf).getBackend();
            StmtExecutor executor = Mockito.mock(StmtExecutor.class);
            Status cancelled = new Status(TStatusCode.CANCELLED, "query cancelled");
            if (cancelBeforeFirstFile) {
                Mockito.when(executor.getPendingCancelReason()).thenReturn(cancelled);
            } else {
                Mockito.when(executor.getPendingCancelReason()).thenReturn(null, cancelled);
            }
            ConnectContext.get().setExecutor(executor);
            BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
            Mockito.when(proxy.fetchTableStructureAsync(Mockito.any(), Mockito.any()))
                    .thenReturn(CompletableFuture.completedFuture(schema("id", TPrimitiveType.INT)));
            try (MockedStatic<BackendServiceProxy> service = Mockito.mockStatic(BackendServiceProxy.class)) {
                service.when(BackendServiceProxy::getInstance).thenReturn(proxy);
                AnalysisException error = Assertions.assertThrows(AnalysisException.class, tvf::getTableColumns);
                Assertions.assertTrue(error.getMessage().contains("query cancelled"));
                Mockito.verify(proxy, Mockito.times(cancelBeforeFirstFile ? 0 : 1))
                        .fetchTableStructureAsync(Mockito.any(), Mockito.any());
            }
        }
    }

    @Test
    public void testMergeSchemaRejectsConflictsAndInvalidProperties() throws Exception {
        S3TableValuedFunction tvf = tvf();
        Map<String, Column> merged = new TreeMap<>();
        tvf.mergeFileSchema(schema("ID", TPrimitiveType.INT), merged, 1);
        tvf.mergeFileSchema(schema("id", TPrimitiveType.INT), merged, 2);
        Assertions.assertEquals(1, merged.size());
        tvf.mergeFileSchema(schema("ä", TPrimitiveType.INT), merged, 2);
        Assertions.assertTrue(merged.containsKey("ä"));
        Assertions.assertEquals(2, merged.size());
        AnalysisException unsupportedName = Assertions.assertThrows(AnalysisException.class,
                () -> tvf.mergeFileSchema(schema("Ä", TPrimitiveType.INT), merged, 3));
        Assertions.assertTrue(unsupportedName.getMessage().contains("Unsupported non-ASCII case conversion"));
        Assertions.assertEquals(2, merged.size());
        Assertions.assertThrows(AnalysisException.class,
                () -> tvf.mergeFileSchema(schema("id", TPrimitiveType.BIGINT), merged, 3));
        PFetchTableSchemaResult duplicate = schema("id", TPrimitiveType.INT).toBuilder()
                .setColumnNums(2).addColumnNames("ID")
                .addColumnTypes(schema("id", TPrimitiveType.INT).getColumnTypes(0)).build();
        Assertions.assertThrows(AnalysisException.class, () -> tvf.mergeFileSchema(duplicate, new TreeMap<>(), 1));
        PFetchTableSchemaResult complex = PFetchTableSchemaResult.newBuilder().setColumnNums(1)
                .addColumnNames("nested").addColumnTypes(PTypeDesc.newBuilder()
                        .addTypes(PTypeNode.newBuilder().setType(TTypeNodeType.STRUCT.getValue())
                                .setScalarType(PScalarType.newBuilder().setType(TPrimitiveType.STRUCT.getValue()))))
                .build();
        Assertions.assertThrows(AnalysisException.class, () -> tvf.mergeFileSchema(complex, new TreeMap<>(), 1));
        Map<String, String> props = new HashMap<>(PROPERTIES);
        props.put("merge_schema", "invalid");
        Assertions.assertThrows(AnalysisException.class, () -> new S3TableValuedFunction(props));
        props.put("merge_schema", "true");
        Assertions.assertThrows(AnalysisException.class, () -> new S3TableValuedFunction(props));
    }

    @Test
    public void testMergeSchemaEmptyAndPartitionConflict() throws Exception {
        Map<String, String> props = new HashMap<>(PROPERTIES);
        props.put("format", "parquet");
        props.put("merge_schema", "true");
        props.put("path_partition_keys", "campaign");
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.getHost()).thenReturn("127.0.0.1");
        Mockito.when(backend.getBrpcPort()).thenReturn(8060);
        S3TableValuedFunction empty = Mockito.spy(new S3TableValuedFunction(props,
                Collections.singletonList(file("empty", 0))));
        Mockito.doReturn(backend).when(empty).getBackend();
        Assertions.assertEquals("__dummy_col", empty.getTableColumns().get(0).getName());

        S3TableValuedFunction conflicting = Mockito.spy(new S3TableValuedFunction(props,
                Collections.singletonList(file("one", 10))));
        Mockito.doReturn(backend).when(conflicting).getBackend();
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        Mockito.when(proxy.fetchTableStructureAsync(Mockito.any(), Mockito.any()))
                .thenReturn(CompletableFuture.completedFuture(schema("CAMPAIGN", TPrimitiveType.STRING)));
        try (MockedStatic<BackendServiceProxy> service = Mockito.mockStatic(BackendServiceProxy.class)) {
            service.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            Assertions.assertThrows(AnalysisException.class, conflicting::getTableColumns);
        }
    }

    @Test
    public void testMergeSchemaParameterizedTypes() throws Exception {
        S3TableValuedFunction tvf = tvf();
        Map<String, Column> merged = new TreeMap<>();
        tvf.mergeFileSchema(parameterizedSchema("amount", TPrimitiveType.DECIMAL64, 0, 12, 2), merged, 1);
        tvf.mergeFileSchema(parameterizedSchema("amount", TPrimitiveType.DECIMAL64, 0, 12, 2), merged, 2);
        Assertions.assertThrows(AnalysisException.class,
                () -> tvf.mergeFileSchema(parameterizedSchema("amount", TPrimitiveType.DECIMAL64, 0, 12, 3),
                        merged, 3));
        Assertions.assertThrows(AnalysisException.class,
                () -> tvf.mergeFileSchema(parameterizedSchema("amount", TPrimitiveType.DECIMAL64, 0, 13, 2),
                        merged, 3));
        tvf.mergeFileSchema(parameterizedSchema("name", TPrimitiveType.VARCHAR, 10, 0, 0), merged, 1);
        Assertions.assertThrows(AnalysisException.class,
                () -> tvf.mergeFileSchema(parameterizedSchema("name", TPrimitiveType.VARCHAR, 20, 0, 0),
                        merged, 2));
        tvf.mergeFileSchema(parameterizedSchema("ts", TPrimitiveType.DATETIMEV2, 0, 0, 3), merged, 1);
        Assertions.assertThrows(AnalysisException.class,
                () -> tvf.mergeFileSchema(parameterizedSchema("ts", TPrimitiveType.DATETIMEV2, 0, 0, 6),
                        merged, 2));
    }

    private static PFetchTableSchemaResult parameterizedSchema(String name, TPrimitiveType type,
            int length, int precision, int scale) {
        return PFetchTableSchemaResult.newBuilder()
                .setStatus(PStatus.newBuilder().setStatusCode(TStatusCode.OK.getValue()))
                .setColumnNums(1).addColumnNames(name)
                .addColumnTypes(PTypeDesc.newBuilder().addTypes(PTypeNode.newBuilder()
                        .setType(TTypeNodeType.SCALAR.getValue())
                        .setScalarType(PScalarType.newBuilder().setType(type.getValue()).setLen(length)
                                .setPrecision(precision).setScale(scale)))).build();
    }

    private static PFetchTableSchemaResult schema(String name, TPrimitiveType type) {
        return PFetchTableSchemaResult.newBuilder()
                .setStatus(PStatus.newBuilder().setStatusCode(TStatusCode.OK.getValue()))
                .setColumnNums(1).addColumnNames(name)
                .addColumnTypes(PTypeDesc.newBuilder().addTypes(PTypeNode.newBuilder()
                        .setType(TTypeNodeType.SCALAR.getValue())
                        .setScalarType(PScalarType.newBuilder().setType(type.getValue())))).build();
    }

    @Test
    public void testExactFilesAreScopedToTvf() throws Exception {
        S3TableValuedFunction first = new S3TableValuedFunction(PROPERTIES,
                Collections.singletonList(file("a.csv", 10)));
        S3TableValuedFunction second = new S3TableValuedFunction(PROPERTIES,
                Collections.singletonList(file("b.csv", 20)));
        Assertions.assertEquals("s3://bucket/logs/a.csv", first.getFileStatuses().get(0).getPath());
        Assertions.assertEquals("s3://bucket/logs/b.csv", second.getFileStatuses().get(0).getPath());
        Assertions.assertTrue(new S3TableValuedFunction(PROPERTIES).getFileStatuses().isEmpty());
    }

    @Test
    public void testExactFilesParticipateInExpressionEquality() {
        S3 first = S3.withFiles(new Properties(PROPERTIES), Collections.singletonList(file("a.csv", 10)));
        S3 same = S3.withFiles(new Properties(PROPERTIES), Collections.singletonList(file("a.csv", 10)));
        S3 other = S3.withFiles(new Properties(PROPERTIES), Collections.singletonList(file("b.csv", 10)));
        Assertions.assertEquals(first, same);
        Assertions.assertEquals(first.hashCode(), same.hashCode());
        Assertions.assertNotEquals(first, other);
        Assertions.assertNotEquals(first, new S3(new Properties(PROPERTIES)));
    }

    private void assertSchemaFailure(TStatusCode code) throws Exception {
        S3TableValuedFunction tvf = tvf();
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        Mockito.when(proxy.fetchTableStructureAsync(Mockito.any(), Mockito.any()))
                .thenReturn(CompletableFuture.completedFuture(result(code)));
        try (MockedStatic<BackendServiceProxy> service = Mockito.mockStatic(BackendServiceProxy.class)) {
            service.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            Assertions.assertThrows(AnalysisException.class, tvf::getTableColumns);
            int attempts = code == TStatusCode.NOT_FOUND ? 2 : 1;
            Mockito.verify(proxy, Mockito.times(attempts))
                    .fetchTableStructureAsync(Mockito.any(), Mockito.any());
            Assertions.assertThrows(AnalysisException.class, tvf::getTableColumns);
            Mockito.verify(proxy, Mockito.times(attempts * 2))
                    .fetchTableStructureAsync(Mockito.any(), Mockito.any());
            Assertions.assertEquals(2, tvf.getFileStatuses().size());
        }
    }

    private S3TableValuedFunction tvf() throws Exception {
        S3TableValuedFunction tvf = Mockito.spy(new S3TableValuedFunction(PROPERTIES,
                Arrays.asList(file("a.csv", 10), file("b.csv", 10))));
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.getHost()).thenReturn("127.0.0.1");
        Mockito.when(backend.getBrpcPort()).thenReturn(8060);
        Mockito.doReturn(backend).when(tvf).getBackend();
        return tvf;
    }

    private static TBrokerFileStatus file(String key, long size) {
        return new TBrokerFileStatus("s3://bucket/logs/" + key, false, size, true);
    }

    private static PFetchTableSchemaResult result(TStatusCode code) {
        return PFetchTableSchemaResult.newBuilder()
                .setStatus(PStatus.newBuilder().setStatusCode(code.getValue()).addErrorMsgs("schema unavailable"))
                .setColumnNums(0).build();
    }
}
