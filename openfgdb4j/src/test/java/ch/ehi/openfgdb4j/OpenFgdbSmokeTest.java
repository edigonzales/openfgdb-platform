package ch.ehi.openfgdb4j;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import org.junit.Test;
import org.junit.Assume;

public class OpenFgdbSmokeTest {

    @Test
    public void domainAndRelationshipAreIdempotent() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Path dbDir = Files.createTempDirectory("openfgdb4j-smoke-").resolve("test.gdb");
        long db = api.create(dbDir.toString());
        try {
            String runtimeInfo = api.getRuntimeInfo();
            String backend = System.getenv("OPENFGDB4J_BACKEND");
            String expectedBackend = (backend == null || backend.isEmpty()) ? "gdal" : backend;
            assertTrue(runtimeInfo.contains("backend=" + expectedBackend));
            boolean realGdal = runtimeInfo.contains("impl=real_gdal");

            api.execSql(db, "CREATE TABLE classa(T_Id INTEGER, color VARCHAR)");
            api.execSql(db, "CREATE TABLE assoc(classa_fk INTEGER)");

            api.createCodedDomain(db, "enum_color", "STRING");
            api.createCodedDomain(db, "enum_color", "STRING");
            api.addCodedValue(db, "enum_color", "rot", "Rot");
            api.addCodedValue(db, "enum_color", "blau", "Blau");
            api.assignDomainToField(db, "classa", "color", "enum_color");
            api.assignDomainToField(db, "classa", "color", "enum_color");

            api.createRelationshipClass(db, "rel_classa_assoc", "classa", "assoc", "T_Id", "classa_fk", "toAssoc", "toClass", "1:n",
                    false, false);
            api.createRelationshipClass(db, "rel_classa_assoc", "classa", "assoc", "T_Id", "classa_fk", "toAssoc", "toClass", "1:n",
                    false, false);
            api.createRelationshipClass(db, "rel_classa_assoc_alias", "classa", "assoc", "T_Id", "classa_fk", "toAssoc", "toClass",
                    "1:n", false, false);

            List<String> domains = api.listDomains(db);
            List<String> relationships = api.listRelationships(db);
            List<String> tables = api.listTableNames(db);

            assertTrue(domains.contains("enum_color"));
            assertTrue(relationships.contains("rel_classa_assoc"));
            if (realGdal) {
                assertTrue(domains.size() >= 1);
                assertTrue(relationships.size() >= 1);
                assertTrue(tables.contains("classa"));
                assertTrue(tables.contains("assoc"));
            } else {
                assertTrue(tables.contains("GDB_Items"));
                assertTrue(tables.contains("GDB_ItemRelationships"));
                assertEquals(1, domains.size());
                assertEquals(1, relationships.size());
                assertEquals(2, countRows(api, db, "GDB_Items"));
            }
        } finally {
            api.close(db);
        }

        assertFalse(Files.exists(dbDir.resolve(".openfgdb-state.tsv")));
        assertFalse(Files.exists(dbDir.resolve(".openfgdb-meta.tsv")));
    }

    @Test
    public void crudWithTypedReads() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Path dbDir = Files.createTempDirectory("openfgdb4j-crud-").resolve("test.gdb");
        long db = api.create(dbDir.toString());
        try {
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
                    try {
                        assertEquals(Integer.valueOf(7), api.rowGetInt32(fetched, "id"));
                        assertEquals(Double.valueOf(12.5), api.rowGetDouble(fetched, "score"));
                        assertEquals("alpha", api.rowGetString(fetched, "name"));
                        assertArrayEquals(new byte[] {1, 2, 3}, api.rowGetBlob(fetched, "payload"));
                    } finally {
                        api.closeRow(fetched);
                    }
                } finally {
                    api.closeCursor(cursor);
                }
            } finally {
                api.closeTable(db, table);
            }
        } finally {
            api.close(db);
        }
    }

    @Test
    public void geometryColumnIsRegisteredAndReadable() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Path dbDir = Files.createTempDirectory("openfgdb4j-geom-").resolve("test.gdb");
        long db = api.create(dbDir.toString());
        try {
            api.execSql(db, "CREATE TABLE t_geom(id INTEGER, shape OFGDB_GEOMETRY(POINT,2056,2) NOT NULL)");
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
                    try {
                        assertEquals(Integer.valueOf(11), api.rowGetInt32(fetched, "id"));
                        byte[] geom = api.rowGetGeometry(fetched);
                        assertTrue(geom != null && geom.length > 0);
                        byte[] blobView = api.rowGetBlob(fetched, "shape");
                        assertTrue(blobView != null && blobView.length > 0);
                    } finally {
                        api.closeRow(fetched);
                    }
                } finally {
                    api.closeCursor(cursor);
                }
            } finally {
                api.closeTable(db, table);
            }
        } finally {
            api.close(db);
        }
    }

    @Test
    public void gdalFailureDoesNotFallback() throws Exception {
        Assume.assumeTrue("1".equals(System.getenv("OPENFGDB4J_GDAL_FORCE_FAIL")));
        OpenFgdb api = new OpenFgdb();
        Path dbDir = Files.createTempDirectory("openfgdb4j-gdal-fail-").resolve("test.gdb");
        try {
            api.create(dbDir.toString());
            fail("Expected OpenFgdbException when forced GDAL failure is enabled");
        } catch (OpenFgdbException expected) {
            assertTrue(expected.getMessage().contains("gdal"));
        }
    }

    @Test
    public void invalidBackendSelectionFailsFast() throws Exception {
        Assume.assumeTrue("invalid".equals(System.getenv("OPENFGDB4J_BACKEND")));
        OpenFgdb api = new OpenFgdb();
        Path dbDir = Files.createTempDirectory("openfgdb4j-backend-invalid-").resolve("test.gdb");
        try {
            api.create(dbDir.toString());
            fail("Expected OpenFgdbException for invalid OPENFGDB4J_BACKEND");
        } catch (OpenFgdbException expected) {
            assertTrue(expected.getMessage().contains("OPENFGDB4J_BACKEND"));
        }
    }

    private static int countRows(OpenFgdb api, long db, String table) throws Exception {
        long tableHandle = api.openTable(db, table);
        try {
            long cursor = api.search(tableHandle, "*", "");
            try {
                int count = 0;
                while (true) {
                    long row = api.fetchRow(cursor);
                    if (row == 0L) {
                        return count;
                    }
                    count++;
                    api.closeRow(row);
                }
            } finally {
                api.closeCursor(cursor);
            }
        } finally {
            api.closeTable(db, tableHandle);
        }
    }

    private static byte[] pointWkb(double x, double y) {
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 8 + 8).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1); // little endian
        buffer.putInt(1);     // WKB Point
        buffer.putDouble(x);
        buffer.putDouble(y);
        return buffer.array();
    }
}
