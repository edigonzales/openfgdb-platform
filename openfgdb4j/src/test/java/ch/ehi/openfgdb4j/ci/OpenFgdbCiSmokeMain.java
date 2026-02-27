package ch.ehi.openfgdb4j.ci;

import ch.ehi.openfgdb4j.OpenFgdb;
import ch.ehi.openfgdb4j.OpenFgdbException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class OpenFgdbCiSmokeMain {
    private OpenFgdbCiSmokeMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("usage: OpenFgdbCiSmokeMain <gdal|adapter|gdal-fail|invalid-backend>");
        }
        String scenario = args[0];
        if ("gdal".equals(scenario) || "adapter".equals(scenario)) {
            runHappyPath(scenario);
            return;
        }
        if ("gdal-fail".equals(scenario)) {
            runGdalFailScenario();
            return;
        }
        if ("invalid-backend".equals(scenario)) {
            runInvalidBackendScenario();
            return;
        }
        throw new IllegalArgumentException("unknown scenario: " + scenario);
    }

    private static void runHappyPath(String expectedBackend) throws Exception {
        OpenFgdb api = new OpenFgdb();
        Path tempRoot = Files.createTempDirectory("openfgdb4j-ci-");
        Path dbDir = tempRoot.resolve("test.gdb");
        long db = api.create(dbDir.toString());
        try {
            String runtimeInfo = api.getRuntimeInfo();
            require(runtimeInfo.contains("backend=" + expectedBackend), "Unexpected backend info: " + runtimeInfo);

            api.execSql(db, "CREATE TABLE classa(T_Id INTEGER, color VARCHAR)");
            api.execSql(db, "CREATE TABLE assoc(classa_fk INTEGER)");
            api.execSql(db, "CREATE TABLE t_geom(id INTEGER, shape OFGDB_GEOMETRY(POINT,2056,2) NOT NULL)");
            api.execSql(db, "CREATE TABLE t_multipoint(id INTEGER, shape OFGDB_GEOMETRY(MULTIPOINT,2056,2) NOT NULL)");
            api.execSql(db, "CREATE TABLE t_line(id INTEGER, shape OFGDB_GEOMETRY(LINE,2056,2) NOT NULL)");
            api.execSql(db, "CREATE TABLE t_multiline(id INTEGER, shape OFGDB_GEOMETRY(MULTILINE,2056,2) NOT NULL)");
            api.execSql(db, "CREATE TABLE t_polygon(id INTEGER, shape OFGDB_GEOMETRY(POLYGON,2056,2) NOT NULL)");
            api.execSql(db, "CREATE TABLE t_multipolygon(id INTEGER, shape OFGDB_GEOMETRY(MULTIPOLYGON,2056,2) NOT NULL)");
            if ("gdal".equals(expectedBackend)) {
                api.execSql(db, "CREATE TABLE t_compoundcurve(id INTEGER, shape OFGDB_GEOMETRY(COMPOUNDCURVE,2056,2) NOT NULL)");
                api.execSql(db, "CREATE TABLE t_multicurve(id INTEGER, shape OFGDB_GEOMETRY(MULTICURVE,2056,2) NOT NULL)");
                api.execSql(db, "CREATE TABLE t_curvepolygon(id INTEGER, shape OFGDB_GEOMETRY(CURVEPOLYGON,2056,2) NOT NULL)");
                api.execSql(db, "CREATE TABLE t_multisurface(id INTEGER, shape OFGDB_GEOMETRY(MULTISURFACE,2056,2) NOT NULL)");
            }

            api.createCodedDomain(db, "enum_color", "STRING");
            api.createCodedDomain(db, "enum_color", "STRING");
            api.addCodedValue(db, "enum_color", "rot", "Rot");
            api.addCodedValue(db, "enum_color", "blau", "Blau");
            api.assignDomainToField(db, "classa", "color", "enum_color");
            api.assignDomainToField(db, "classa", "color", "enum_color");

            api.createRelationshipClass(db, "rel_classa_assoc", "classa", "assoc", "T_Id", "classa_fk", "toAssoc", "toClass", "1:n", false,
                    false);
            api.createRelationshipClass(db, "rel_classa_assoc", "classa", "assoc", "T_Id", "classa_fk", "toAssoc", "toClass", "1:n", false,
                    false);

            List<String> domains = api.listDomains(db);
            List<String> relationships = api.listRelationships(db);
            require(domains.contains("enum_color"), "Domain enum_color missing");
            require(relationships.contains("rel_classa_assoc"), "Relationship rel_classa_assoc missing");

            runCrudRoundtrip(api, db);
            if ("gdal".equals(expectedBackend)) {
                runNotNullAttributeChecks(api, db);
            }
            runGeometryRoundtrip(api, db);
            runMultipointGeometryRoundtrip(api, db);
            runMultilineGeometryRoundtrip(api, db);
            runMultipolygonGeometryRoundtrip(api, db);
            if ("gdal".equals(expectedBackend)) {
                runCompoundcurveGeometryRoundtrip(api, db);
                runMulticurveGeometryRoundtrip(api, db);
                runCurvepolygonGeometryRoundtrip(api, db);
                runMultisurfaceGeometryRoundtrip(api, db);
                runStrictMismatchChecks(api, db);
                db = runDeclaredGeometryContractReopenRoundtrip(api, db, dbDir.toString());
            }
        } finally {
            api.close(db);
            deleteTreeQuiet(tempRoot);
        }
    }

    private static void runNotNullAttributeChecks(OpenFgdb api, long db) throws Exception {
        api.execSql(db, "CREATE TABLE t_notnull_ci(id INTEGER NOT NULL, name VARCHAR NOT NULL, opt VARCHAR)");
        api.execSql(db, "INSERT INTO t_notnull_ci(id, name) VALUES (1, 'alpha')");

        try {
            api.execSql(db, "INSERT INTO t_notnull_ci(id, opt) VALUES (2, 'x')");
            throw new IllegalStateException("Expected OpenFgdbException for missing NOT NULL column in SQL insert");
        } catch (OpenFgdbException expected) {
            require(expected.getErrorCode() == OpenFgdb.OFGDB_ERR_INVALID_ARG,
                    "Expected OFGDB_ERR_INVALID_ARG for NOT NULL SQL insert violation, got " + expected.getErrorCode());
            String msg = expected.getMessage().toLowerCase();
            require(msg.contains("not null"), "Expected NOT NULL marker in SQL insert violation message: " + expected.getMessage());
            require(msg.contains("name"), "Expected column name in SQL insert violation message: " + expected.getMessage());
        }

        long table = api.openTable(db, "t_notnull_ci");
        try {
            long cursor = api.search(table, "*", "");
            try {
                long fetched = api.fetchRow(cursor);
                require(fetched != 0L, "No row returned for NOT NULL update check");
                try {
                    api.setNull(fetched, "name");
                    try {
                        api.update(table, fetched);
                        throw new IllegalStateException("Expected OpenFgdbException for update-to-NULL on NOT NULL column");
                    } catch (OpenFgdbException expected) {
                        require(expected.getErrorCode() == OpenFgdb.OFGDB_ERR_INVALID_ARG,
                                "Expected OFGDB_ERR_INVALID_ARG for NOT NULL update violation, got " + expected.getErrorCode());
                        String msg = expected.getMessage().toLowerCase();
                        require(msg.contains("not null"),
                                "Expected NOT NULL marker in update violation message: " + expected.getMessage());
                        require(msg.contains("name"),
                                "Expected column name in update violation message: " + expected.getMessage());
                    }
                } finally {
                    api.closeRow(fetched);
                }
            } finally {
                api.closeCursor(cursor);
            }
        } finally {
            api.closeTable(db, table);
        }
    }

    private static void runCrudRoundtrip(OpenFgdb api, long db) throws Exception {
        api.execSql(db, "CREATE TABLE t_test(id INTEGER, score DOUBLE, name VARCHAR, payload BLOB)");
        long table = api.openTable(db, "t_test");
        try {
            long row = api.createRow(table);
            try {
                api.setInt32(row, "id", 7);
                api.setDouble(row, "score", 12.5);
                api.setString(row, "name", "alpha");
                api.setBlob(row, "payload", new byte[] {1, 2, 3});
                api.insert(table, row);
            } finally {
                api.closeRow(row);
            }

            long cursor = api.search(table, "*", "");
            try {
                long fetched = api.fetchRow(cursor);
                require(fetched != 0L, "No row returned for CRUD roundtrip");
                try {
                    require(Integer.valueOf(7).equals(api.rowGetInt32(fetched, "id")), "id mismatch");
                    require(Double.valueOf(12.5).equals(api.rowGetDouble(fetched, "score")), "score mismatch");
                    require("alpha".equals(api.rowGetString(fetched, "name")), "name mismatch");
                    byte[] payload = api.rowGetBlob(fetched, "payload");
                    require(payload != null && payload.length == 3, "payload mismatch");
                } finally {
                    api.closeRow(fetched);
                }
            } finally {
                api.closeCursor(cursor);
            }
        } finally {
            api.closeTable(db, table);
        }
    }

    private static void runGeometryRoundtrip(OpenFgdb api, long db) throws Exception {
        long table = api.openTable(db, "t_geom");
        try {
            long row = api.createRow(table);
            try {
                api.setInt32(row, "id", 11);
                api.setGeometry(row, pointWkb(2600000.5, 1200000.25));
                api.insert(table, row);
            } finally {
                api.closeRow(row);
            }

            long cursor = api.search(table, "*", "");
            try {
                long fetched = api.fetchRow(cursor);
                require(fetched != 0L, "No row returned for geometry roundtrip");
                try {
                    byte[] wkb = api.rowGetGeometry(fetched);
                    require(wkb != null && wkb.length > 0, "rowGetGeometry returned empty value");
                    require(wkbType(wkb) == 1, "unexpected WKB type for point rowGetGeometry: " + wkbType(wkb));
                } finally {
                    api.closeRow(fetched);
                }
            } finally {
                api.closeCursor(cursor);
            }
        } finally {
            api.closeTable(db, table);
        }
    }

    private static void runMultipointGeometryRoundtrip(OpenFgdb api, long db) throws Exception {
        long table = api.openTable(db, "t_multipoint");
        try {
            long row = api.createRow(table);
            try {
                api.setInt32(row, "id", 12);
                api.setGeometry(row, multiPointWkb(new double[][] {{2600000.5, 1200000.25}, {2600001.5, 1200001.25}}));
                api.insert(table, row);
            } finally {
                api.closeRow(row);
            }

            long cursor = api.search(table, "*", "");
            try {
                long fetched = api.fetchRow(cursor);
                require(fetched != 0L, "No row returned for multipoint roundtrip");
                try {
                    byte[] wkb = api.rowGetGeometry(fetched);
                    require(wkb != null && wkb.length > 0, "rowGetGeometry returned empty value for multipoint");
                    require(wkbType(wkb) == 4, "unexpected WKB type for multipoint rowGetGeometry: " + wkbType(wkb));
                } finally {
                    api.closeRow(fetched);
                }
            } finally {
                api.closeCursor(cursor);
            }
        } finally {
            api.closeTable(db, table);
        }
    }

    private static void runMultilineGeometryRoundtrip(OpenFgdb api, long db) throws Exception {
        long table = api.openTable(db, "t_multiline");
        try {
            long row = api.createRow(table);
            try {
                api.setInt32(row, "id", 13);
                api.setGeometry(row, multiLineStringWkb(new double[][][] {
                        {{2600000.0, 1200000.0}, {2600002.0, 1200001.0}},
                        {{2600010.0, 1200010.0}, {2600012.0, 1200011.0}}
                }));
                api.insert(table, row);
            } finally {
                api.closeRow(row);
            }

            long cursor = api.search(table, "*", "");
            try {
                long fetched = api.fetchRow(cursor);
                require(fetched != 0L, "No row returned for multiline roundtrip");
                try {
                    byte[] wkb = api.rowGetGeometry(fetched);
                    require(wkb != null && wkb.length > 0, "rowGetGeometry returned empty value for multiline");
                    require(wkbType(wkb) == 5, "unexpected WKB type for multiline rowGetGeometry: " + wkbType(wkb));
                } finally {
                    api.closeRow(fetched);
                }
            } finally {
                api.closeCursor(cursor);
            }
        } finally {
            api.closeTable(db, table);
        }
    }

    private static void runMultipolygonGeometryRoundtrip(OpenFgdb api, long db) throws Exception {
        long table = api.openTable(db, "t_multipolygon");
        try {
            long row = api.createRow(table);
            try {
                api.setInt32(row, "id", 14);
                api.setGeometry(row, multiPolygonWkb(new double[][][][] {
                        {
                                {{2600000.0, 1200000.0}, {2600010.0, 1200000.0}, {2600010.0, 1200010.0}, {2600000.0, 1200010.0}, {2600000.0, 1200000.0}}
                        },
                        {
                                {{2600020.0, 1200020.0}, {2600030.0, 1200020.0}, {2600030.0, 1200030.0}, {2600020.0, 1200030.0}, {2600020.0, 1200020.0}}
                        }
                }));
                api.insert(table, row);
            } finally {
                api.closeRow(row);
            }

            long cursor = api.search(table, "*", "");
            try {
                long fetched = api.fetchRow(cursor);
                require(fetched != 0L, "No row returned for multipolygon roundtrip");
                try {
                    byte[] wkb = api.rowGetGeometry(fetched);
                    require(wkb != null && wkb.length > 0, "rowGetGeometry returned empty value for multipolygon");
                    require(wkbType(wkb) == 6, "unexpected WKB type for multipolygon rowGetGeometry: " + wkbType(wkb));
                } finally {
                    api.closeRow(fetched);
                }
            } finally {
                api.closeCursor(cursor);
            }
        } finally {
            api.closeTable(db, table);
        }
    }

    private static void runCompoundcurveGeometryRoundtrip(OpenFgdb api, long db) throws Exception {
        long table = api.openTable(db, "t_compoundcurve");
        try {
            long row = api.createRow(table);
            try {
                api.setInt32(row, "id", 15);
                api.setGeometry(row, compoundCurveWkbFromComponents(new byte[][] {
                        circularStringWkb(new double[][] {
                                {2600000.0, 1200000.0},
                                {2600002.0, 1200001.0},
                                {2600004.0, 1200002.0}
                        }),
                        lineStringWkb(new double[][] {
                                {2600004.0, 1200002.0},
                                {2600006.0, 1200003.0}
                        })
                }));
                api.insert(table, row);
            } finally {
                api.closeRow(row);
            }

            long cursor = api.search(table, "*", "");
            try {
                long fetched = api.fetchRow(cursor);
                require(fetched != 0L, "No row returned for compoundcurve roundtrip");
                try {
                    byte[] wkb = api.rowGetGeometry(fetched);
                    require(wkb != null && wkb.length > 0, "rowGetGeometry returned empty value for compoundcurve");
                    require(wkbType(wkb) == 9, "unexpected WKB type for compoundcurve rowGetGeometry: " + wkbType(wkb));
                } finally {
                    api.closeRow(fetched);
                }
            } finally {
                api.closeCursor(cursor);
            }
        } finally {
            api.closeTable(db, table);
        }
    }

    private static void runMulticurveGeometryRoundtrip(OpenFgdb api, long db) throws Exception {
        long table = api.openTable(db, "t_multicurve");
        try {
            long row = api.createRow(table);
            try {
                api.setInt32(row, "id", 16);
                api.setGeometry(row, multiCurveWkb(new byte[][] {
                        compoundCurveWkb(new double[][][] {
                                {{2600000.0, 1200000.0}, {2600001.0, 1200001.0}, {2600002.0, 1200002.0}}
                        }),
                        lineStringWkb(new double[][] {{2600010.0, 1200010.0}, {2600012.0, 1200011.0}})
                }));
                api.insert(table, row);
            } finally {
                api.closeRow(row);
            }

            long cursor = api.search(table, "*", "");
            try {
                long fetched = api.fetchRow(cursor);
                require(fetched != 0L, "No row returned for multicurve roundtrip");
                try {
                    byte[] wkb = api.rowGetGeometry(fetched);
                    require(wkb != null && wkb.length > 0, "rowGetGeometry returned empty value for multicurve");
                    require(wkbType(wkb) == 11, "unexpected WKB type for multicurve rowGetGeometry: " + wkbType(wkb));
                } finally {
                    api.closeRow(fetched);
                }
            } finally {
                api.closeCursor(cursor);
            }
        } finally {
            api.closeTable(db, table);
        }
    }

    private static void runCurvepolygonGeometryRoundtrip(OpenFgdb api, long db) throws Exception {
        long table = api.openTable(db, "t_curvepolygon");
        try {
            long row = api.createRow(table);
            try {
                api.setInt32(row, "id", 17);
                api.setGeometry(row, curvePolygonWkb(new byte[][] {
                        compoundCurveWkbFromComponents(new byte[][] {
                                circularStringWkb(new double[][] {
                                        {2600000.0, 1200000.0},
                                        {2600002.0, 1200002.0},
                                        {2600004.0, 1200000.0}
                                }),
                                lineStringWkb(new double[][] {
                                        {2600004.0, 1200000.0},
                                        {2600000.0, 1200000.0}
                                })
                        })
                }));
                api.insert(table, row);
            } finally {
                api.closeRow(row);
            }

            long cursor = api.search(table, "*", "");
            try {
                long fetched = api.fetchRow(cursor);
                require(fetched != 0L, "No row returned for curvepolygon roundtrip");
                try {
                    byte[] wkb = api.rowGetGeometry(fetched);
                    require(wkb != null && wkb.length > 0, "rowGetGeometry returned empty value for curvepolygon");
                    require(wkbType(wkb) == 10, "unexpected WKB type for curvepolygon rowGetGeometry: " + wkbType(wkb));
                    assertWkbContainsExpectedCoords(wkb, new double[][] {
                            {2600000.0, 1200000.0},
                            {2600002.0, 1200002.0},
                            {2600004.0, 1200000.0}
                    });
                } finally {
                    api.closeRow(fetched);
                }
            } finally {
                api.closeCursor(cursor);
            }
        } finally {
            api.closeTable(db, table);
        }
    }

    private static void runMultisurfaceGeometryRoundtrip(OpenFgdb api, long db) throws Exception {
        long table = api.openTable(db, "t_multisurface");
        try {
            long row = api.createRow(table);
            try {
                api.setInt32(row, "id", 18);
                api.setGeometry(row, multiSurfaceWkb(new byte[][] {
                        curvePolygonWkb(new byte[][] {
                                compoundCurveWkbFromComponents(new byte[][] {
                                        circularStringWkb(new double[][] {
                                                {2600000.0, 1200000.0},
                                                {2600002.0, 1200002.0},
                                                {2600004.0, 1200000.0}
                                        }),
                                        lineStringWkb(new double[][] {
                                                {2600004.0, 1200000.0},
                                                {2600000.0, 1200000.0}
                                        })
                                })
                        })
                }));
                api.insert(table, row);
            } finally {
                api.closeRow(row);
            }

            long cursor = api.search(table, "*", "");
            try {
                long fetched = api.fetchRow(cursor);
                require(fetched != 0L, "No row returned for multisurface roundtrip");
                try {
                    byte[] wkb = api.rowGetGeometry(fetched);
                    require(wkb != null && wkb.length > 0, "rowGetGeometry returned empty value for multisurface");
                    require(wkbType(wkb) == 12, "unexpected WKB type for multisurface rowGetGeometry: " + wkbType(wkb));
                    assertWkbContainsExpectedCoords(wkb, new double[][] {
                            {2600000.0, 1200000.0},
                            {2600002.0, 1200002.0},
                            {2600004.0, 1200000.0}
                    });
                } finally {
                    api.closeRow(fetched);
                }
            } finally {
                api.closeCursor(cursor);
            }
        } finally {
            api.closeTable(db, table);
        }
    }

    private static void runStrictMismatchChecks(OpenFgdb api, long db) throws Exception {
        assertSetGeometryRejects(api, db, "t_line", multiLineStringWkb(new double[][][] {{{2600000.0, 1200000.0}, {2600002.0, 1200001.0}}}),
                "expected LINE", "got MULTILINE");
        assertSetGeometryRejects(api, db, "t_multiline", lineStringWkb(new double[][] {{2600000.0, 1200000.0}, {2600002.0, 1200001.0}}),
                "expected MULTILINE", "got LINE");
        assertSetGeometryRejects(api, db, "t_polygon", multiPolygonWkb(new double[][][][] {
                {
                        {{2600000.0, 1200000.0}, {2600010.0, 1200000.0}, {2600010.0, 1200010.0}, {2600000.0, 1200010.0}, {2600000.0, 1200000.0}}
                }
        }), "expected POLYGON", "got MULTIPOLYGON");
        assertSetGeometryRejects(api, db, "t_multipolygon", polygonWkb(new double[][][] {
                {{2600000.0, 1200000.0}, {2600010.0, 1200000.0}, {2600010.0, 1200010.0}, {2600000.0, 1200010.0}, {2600000.0, 1200000.0}}
        }), "expected MULTIPOLYGON", "got POLYGON");
        assertSetGeometryRejects(api, db, "t_line", compoundCurveWkb(new double[][][] {
                {{2600000.0, 1200000.0}, {2600002.0, 1200001.0}, {2600004.0, 1200002.0}}
        }), "expected LINE", "got COMPOUNDCURVE");
        assertSetGeometryRejects(api, db, "t_compoundcurve", lineStringWkb(new double[][] {{2600000.0, 1200000.0}, {2600002.0, 1200001.0}}),
                "expected COMPOUNDCURVE", "got LINE");
        assertSetGeometryRejects(api, db, "t_polygon", curvePolygonWkb(new byte[][] {
                lineStringWkb(new double[][] {
                        {2600000.0, 1200000.0},
                        {2600010.0, 1200000.0},
                        {2600010.0, 1200010.0},
                        {2600000.0, 1200010.0},
                        {2600000.0, 1200000.0}
                })
        }), "expected POLYGON", "got CURVEPOLYGON");
        assertSetGeometryRejects(api, db, "t_curvepolygon", polygonWkb(new double[][][] {
                {{2600000.0, 1200000.0}, {2600010.0, 1200000.0}, {2600010.0, 1200010.0}, {2600000.0, 1200010.0}, {2600000.0, 1200000.0}}
        }), "expected CURVEPOLYGON", "got POLYGON");
    }

    private static void assertSetGeometryRejects(OpenFgdb api, long db, String tableName, byte[] wkb, String expected, String got)
            throws Exception {
        long table = api.openTable(db, tableName);
        try {
            long row = api.createRow(table);
            try {
                try {
                    api.setGeometry(row, wkb);
                    throw new IllegalStateException("Expected OpenFgdbException for geometry type mismatch in " + tableName);
                } catch (OpenFgdbException ex) {
                    require(ex.getErrorCode() == OpenFgdb.OFGDB_ERR_INVALID_ARG,
                            "Expected OFGDB_ERR_INVALID_ARG for " + tableName + " but got " + ex.getErrorCode());
                    require(ex.getMessage().contains(expected), "Missing expected token in message: " + ex.getMessage());
                    require(ex.getMessage().contains(got), "Missing actual token in message: " + ex.getMessage());
                }
            } finally {
                api.closeRow(row);
            }
        } finally {
            api.closeTable(db, table);
        }
    }

    private static long runDeclaredGeometryContractReopenRoundtrip(OpenFgdb api, long db, String dbPath) throws Exception {
        api.execSql(db, "CREATE TABLE t_declared_reopen(id INTEGER, geom_decl OFGDB_GEOMETRY(LINE,2056,2) NOT NULL)");
        long table = api.openTable(db, "t_declared_reopen");
        try {
            long row = api.createRow(table);
            try {
                api.setInt32(row, "id", 21);
                api.setGeometry(row, lineStringWkb(new double[][] {{2600000.0, 1200000.0}, {2600002.0, 1200001.0}}));
                api.insert(table, row);
            } finally {
                api.closeRow(row);
            }
        } finally {
            api.closeTable(db, table);
        }

        api.close(db);
        long reopenedDb = api.open(dbPath);
        long reopenedTable = api.openTable(reopenedDb, "t_declared_reopen");
        try {
            long cursor = api.search(reopenedTable, "*", "");
            try {
                long fetched = api.fetchRow(cursor);
                require(fetched != 0L, "No row returned for declared geometry reopen roundtrip");
                try {
                    require(!api.rowIsNull(fetched, "geom_decl"), "declared geometry column unexpectedly null after reopen");
                    byte[] blob = api.rowGetBlob(fetched, "geom_decl");
                    require(blob != null && blob.length > 0, "declared geometry blob missing after reopen");
                    require(wkbType(blob) == 2, "unexpected WKB type for declared geometry after reopen: " + wkbType(blob));
                } finally {
                    api.closeRow(fetched);
                }
            } finally {
                api.closeCursor(cursor);
            }
        } finally {
            api.closeTable(reopenedDb, reopenedTable);
        }
        return reopenedDb;
    }

    private static void runGdalFailScenario() throws IOException {
        OpenFgdb api = new OpenFgdb();
        Path tempRoot = Files.createTempDirectory("openfgdb4j-ci-gdal-fail-");
        Path dbDir = tempRoot.resolve("test.gdb");
        try {
            api.create(dbDir.toString());
            throw new IllegalStateException("Expected OpenFgdbException when OPENFGDB4J_GDAL_FORCE_FAIL=1");
        } catch (OpenFgdbException expected) {
            require(expected.getMessage().toLowerCase().contains("gdal"), "Missing gdal hint in error: " + expected.getMessage());
        } finally {
            deleteTreeQuiet(tempRoot);
        }
    }

    private static void runInvalidBackendScenario() throws IOException {
        OpenFgdb api = new OpenFgdb();
        Path tempRoot = Files.createTempDirectory("openfgdb4j-ci-invalid-backend-");
        Path dbDir = tempRoot.resolve("test.gdb");
        try {
            api.create(dbDir.toString());
            throw new IllegalStateException("Expected OpenFgdbException for invalid OPENFGDB4J_BACKEND");
        } catch (OpenFgdbException expected) {
            require(expected.getMessage().contains("OPENFGDB4J_BACKEND"),
                    "Missing OPENFGDB4J_BACKEND hint in error: " + expected.getMessage());
        } finally {
            deleteTreeQuiet(tempRoot);
        }
    }

    private static byte[] pointWkb(double x, double y) {
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 8 + 8).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1);
        buffer.putInt(1);
        buffer.putDouble(x);
        buffer.putDouble(y);
        return buffer.array();
    }

    private static byte[] multiPointWkb(double[][] points) {
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 4 + points.length * (1 + 4 + 8 + 8)).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1);
        buffer.putInt(4);
        buffer.putInt(points.length);
        for (double[] point : points) {
            buffer.put((byte) 1);
            buffer.putInt(1);
            buffer.putDouble(point[0]);
            buffer.putDouble(point[1]);
        }
        return buffer.array();
    }

    private static byte[] lineStringWkb(double[][] points) {
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 4 + points.length * 16).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1);
        buffer.putInt(2);
        buffer.putInt(points.length);
        for (double[] point : points) {
            buffer.putDouble(point[0]);
            buffer.putDouble(point[1]);
        }
        return buffer.array();
    }

    private static byte[] multiLineStringWkb(double[][][] lines) {
        int size = 1 + 4 + 4;
        for (double[][] line : lines) {
            size += 1 + 4 + 4 + line.length * 16;
        }
        ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1);
        buffer.putInt(5);
        buffer.putInt(lines.length);
        for (double[][] line : lines) {
            buffer.put((byte) 1);
            buffer.putInt(2);
            buffer.putInt(line.length);
            for (double[] point : line) {
                buffer.putDouble(point[0]);
                buffer.putDouble(point[1]);
            }
        }
        return buffer.array();
    }

    private static byte[] polygonWkb(double[][][] rings) {
        int size = 1 + 4 + 4;
        for (double[][] ring : rings) {
            size += 4 + ring.length * 16;
        }
        ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1);
        buffer.putInt(3);
        buffer.putInt(rings.length);
        for (double[][] ring : rings) {
            buffer.putInt(ring.length);
            for (double[] point : ring) {
                buffer.putDouble(point[0]);
                buffer.putDouble(point[1]);
            }
        }
        return buffer.array();
    }

    private static byte[] multiPolygonWkb(double[][][][] polygons) {
        int size = 1 + 4 + 4;
        for (double[][][] polygon : polygons) {
            size += 1 + 4 + 4;
            for (double[][] ring : polygon) {
                size += 4 + ring.length * 16;
            }
        }
        ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1);
        buffer.putInt(6);
        buffer.putInt(polygons.length);
        for (double[][][] polygon : polygons) {
            buffer.put((byte) 1);
            buffer.putInt(3);
            buffer.putInt(polygon.length);
            for (double[][] ring : polygon) {
                buffer.putInt(ring.length);
                for (double[] point : ring) {
                    buffer.putDouble(point[0]);
                    buffer.putDouble(point[1]);
                }
            }
        }
        return buffer.array();
    }

    private static byte[] circularStringWkb(double[][] points) {
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 4 + points.length * 16).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1);
        buffer.putInt(8);
        buffer.putInt(points.length);
        for (double[] point : points) {
            buffer.putDouble(point[0]);
            buffer.putDouble(point[1]);
        }
        return buffer.array();
    }

    private static byte[] compoundCurveWkb(double[][][] curves) {
        byte[][] curveWkbs = new byte[curves.length][];
        for (int i = 0; i < curves.length; i++) {
            curveWkbs[i] = lineStringWkb(curves[i]);
        }
        return compoundCurveWkbFromComponents(curveWkbs);
    }

    private static byte[] compoundCurveWkbFromComponents(byte[][] components) {
        int size = 1 + 4 + 4;
        for (byte[] component : components) {
            size += component.length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1);
        buffer.putInt(9);
        buffer.putInt(components.length);
        for (byte[] component : components) {
            buffer.put(component);
        }
        return buffer.array();
    }

    private static byte[] multiCurveWkb(byte[][] curves) {
        int size = 1 + 4 + 4;
        for (byte[] curve : curves) {
            size += curve.length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1);
        buffer.putInt(11);
        buffer.putInt(curves.length);
        for (byte[] curve : curves) {
            buffer.put(curve);
        }
        return buffer.array();
    }

    private static byte[] curvePolygonWkb(byte[][] rings) {
        int size = 1 + 4 + 4;
        for (byte[] ring : rings) {
            size += ring.length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1);
        buffer.putInt(10);
        buffer.putInt(rings.length);
        for (byte[] ring : rings) {
            buffer.put(ring);
        }
        return buffer.array();
    }

    private static byte[] multiSurfaceWkb(byte[][] surfaces) {
        int size = 1 + 4 + 4;
        for (byte[] surface : surfaces) {
            size += surface.length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1);
        buffer.putInt(12);
        buffer.putInt(surfaces.length);
        for (byte[] surface : surfaces) {
            buffer.put(surface);
        }
        return buffer.array();
    }

    private static boolean containsCoordinatePairLe(byte[] wkb, double x, double y) {
        if (wkb == null || wkb.length < 16) {
            return false;
        }
        for (int i = 0; i <= wkb.length - 16; i++) {
            double xi = ByteBuffer.wrap(wkb, i, 8).order(ByteOrder.LITTLE_ENDIAN).getDouble();
            if (Math.abs(xi - x) > 1e-9) {
                continue;
            }
            double yi = ByteBuffer.wrap(wkb, i + 8, 8).order(ByteOrder.LITTLE_ENDIAN).getDouble();
            if (Math.abs(yi - y) <= 1e-9) {
                return true;
            }
        }
        return false;
    }

    private static void assertWkbContainsExpectedCoords(byte[] wkb, double[][] coords) {
        for (double[] coord : coords) {
            require(containsCoordinatePairLe(wkb, coord[0], coord[1]),
                    "missing coordinate pair " + coord[0] + "," + coord[1]);
        }
    }

    private static int wkbType(byte[] wkb) {
        if (wkb == null || wkb.length < 5) {
            return -1;
        }
        ByteOrder order;
        if (wkb[0] == 0) {
            order = ByteOrder.BIG_ENDIAN;
        } else {
            order = ByteOrder.LITTLE_ENDIAN;
        }
        return ByteBuffer.wrap(wkb, 1, 4).order(order).getInt();
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    private static void deleteTreeQuiet(Path root) {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try {
            Files.walk(root)
                    .sorted((a, b) -> b.compareTo(a))
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException ignored) {
                        }
                    });
        } catch (IOException ignored) {
        }
    }
}
