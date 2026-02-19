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
            runGeometryRoundtrip(api, db);
        } finally {
            api.close(db);
            deleteTreeQuiet(tempRoot);
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
