package ch.ehi.openfgdb4j;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.Assume;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

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
    public void varcharLengthIsPersistedInDefinition() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Assume.assumeTrue(isRealGdal(api));
        Path dbDir = Files.createTempDirectory("openfgdb4j-varchar-length-").resolve("test.gdb");
        long dbCreate = api.create(dbDir.toString());
        try {
            api.execSql(dbCreate, "CREATE TABLE t_len(id INTEGER, name VARCHAR(20), note VARCHAR)");
        } finally {
            api.close(dbCreate);
        }

        long dbOpen = api.open(dbDir.toString());
        try {
            String definitionXml = readDefinitionXml(api, dbOpen, "t_len");
            assertTrue(definitionXml != null && !definitionXml.isEmpty());
            assertEquals(Integer.valueOf(20), findFieldLength(definitionXml, "name"));
            Integer noteLength = findFieldLength(definitionXml, "note");
            assertTrue(noteLength == null || noteLength.intValue() == 0);
        } finally {
            api.close(dbOpen);
        }
    }

    @Test
    public void deleteByEqWorks() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Assume.assumeTrue(isRealGdal(api));
        Path dbDir = Files.createTempDirectory("openfgdb4j-delete-eq-").resolve("test.gdb");
        long db = api.create(dbDir.toString());
        try {
            api.execSql(db, "CREATE TABLE t_delete_eq(id INTEGER, t_id INTEGER)");
            api.execSql(db, "INSERT INTO t_delete_eq(id, t_id) VALUES (1, 1)");
            api.execSql(db, "INSERT INTO t_delete_eq(id, t_id) VALUES (2, 2)");
            api.execSql(db, "INSERT INTO t_delete_eq(id, t_id) VALUES (3, 3)");
            assertEquals(3, countRows(api, db, "t_delete_eq"));

            api.execSql(db, "DELETE FROM t_delete_eq WHERE t_id=1");
            assertEquals(2, countRows(api, db, "t_delete_eq"));
        } finally {
            api.close(db);
        }
    }

    @Test
    public void deleteByInWorks() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Assume.assumeTrue(isRealGdal(api));
        Path dbDir = Files.createTempDirectory("openfgdb4j-delete-in-").resolve("test.gdb");
        long db = api.create(dbDir.toString());
        try {
            api.execSql(db, "CREATE TABLE t_delete_in(id INTEGER, t_id INTEGER)");
            api.execSql(db, "INSERT INTO t_delete_in(id, t_id) VALUES (1, 1)");
            api.execSql(db, "INSERT INTO t_delete_in(id, t_id) VALUES (2, 2)");
            api.execSql(db, "INSERT INTO t_delete_in(id, t_id) VALUES (3, 3)");
            api.execSql(db, "INSERT INTO t_delete_in(id, t_id) VALUES (4, 4)");
            assertEquals(4, countRows(api, db, "t_delete_in"));

            api.execSql(db, "DELETE FROM t_delete_in WHERE t_id IN (2,3)");
            assertEquals(2, countRows(api, db, "t_delete_in"));
        } finally {
            api.close(db);
        }
    }

    @Test
    public void deleteNoWhereWorks() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Assume.assumeTrue(isRealGdal(api));
        Path dbDir = Files.createTempDirectory("openfgdb4j-delete-all-").resolve("test.gdb");
        long db = api.create(dbDir.toString());
        try {
            api.execSql(db, "CREATE TABLE t_delete_all(id INTEGER, t_id INTEGER)");
            api.execSql(db, "INSERT INTO t_delete_all(id, t_id) VALUES (1, 1)");
            api.execSql(db, "INSERT INTO t_delete_all(id, t_id) VALUES (2, 2)");
            assertEquals(2, countRows(api, db, "t_delete_all"));

            api.execSql(db, "DELETE FROM t_delete_all");
            assertEquals(0, countRows(api, db, "t_delete_all"));
        } finally {
            api.close(db);
        }
    }

    @Test
    public void attributeNotNullIsEnforcedInSqlInsert() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Assume.assumeTrue(isRealGdal(api));
        Path dbDir = Files.createTempDirectory("openfgdb4j-notnull-sql-").resolve("test.gdb");
        long db = api.create(dbDir.toString());
        try {
            api.execSql(db, "CREATE TABLE t_nn(id INTEGER NOT NULL, name VARCHAR NOT NULL, opt VARCHAR)");

            try {
                api.execSql(db, "INSERT INTO t_nn(id, opt) VALUES (1, 'x')");
                fail("Expected OpenFgdbException for missing NOT NULL column");
            } catch (OpenFgdbException expected) {
                assertNotNullViolation(expected, "name");
            }

            try {
                api.execSql(db, "INSERT INTO t_nn(id, name, opt) VALUES (2, NULL, 'x')");
                fail("Expected OpenFgdbException for explicit NULL in NOT NULL column");
            } catch (OpenFgdbException expected) {
                assertNotNullViolation(expected, "name");
            }
        } finally {
            api.close(db);
        }
    }

    @Test
    public void attributeNotNullIsEnforcedInRowApiInsert() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Assume.assumeTrue(isRealGdal(api));
        Path dbDir = Files.createTempDirectory("openfgdb4j-notnull-row-").resolve("test.gdb");
        long db = api.create(dbDir.toString());
        try {
            api.execSql(db, "CREATE TABLE t_nn_row(id INTEGER NOT NULL, name VARCHAR NOT NULL, opt VARCHAR)");
            long table = api.openTable(db, "t_nn_row");
            try {
                long row = api.createRow(table);
                try {
                    api.setInt32(row, "id", 1);
                    try {
                        api.insert(table, row);
                        fail("Expected OpenFgdbException for missing NOT NULL column in row API insert");
                    } catch (OpenFgdbException expected) {
                        assertNotNullViolation(expected, "name");
                    }
                } finally {
                    api.closeRow(row);
                }
            } finally {
                api.closeTable(db, table);
            }
        } finally {
            api.close(db);
        }
    }

    @Test
    public void attributeNotNullAllowsNullableColumns() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Assume.assumeTrue(isRealGdal(api));
        Path dbDir = Files.createTempDirectory("openfgdb4j-notnull-nullable-").resolve("test.gdb");
        long db = api.create(dbDir.toString());
        try {
            api.execSql(db, "CREATE TABLE t_nn_ok(id INTEGER NOT NULL, name VARCHAR NOT NULL, opt VARCHAR)");
            api.execSql(db, "INSERT INTO t_nn_ok(id, name) VALUES (1, 'alpha')");

            long table = api.openTable(db, "t_nn_ok");
            try {
                long cursor = api.search(table, "*", "");
                try {
                    long fetched = api.fetchRow(cursor);
                    assertTrue(fetched != 0L);
                    try {
                        assertEquals(Integer.valueOf(1), api.rowGetInt32(fetched, "id"));
                        assertEquals("alpha", api.rowGetString(fetched, "name"));
                        assertTrue(api.rowIsNull(fetched, "opt"));
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
    public void updateToNullOnNotNullColumnFails() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Assume.assumeTrue(isRealGdal(api));
        Path dbDir = Files.createTempDirectory("openfgdb4j-notnull-update-").resolve("test.gdb");
        long db = api.create(dbDir.toString());
        try {
            api.execSql(db, "CREATE TABLE t_nn_upd(id INTEGER NOT NULL, name VARCHAR NOT NULL, opt VARCHAR)");
            api.execSql(db, "INSERT INTO t_nn_upd(id, name, opt) VALUES (1, 'alpha', 'x')");

            long table = api.openTable(db, "t_nn_upd");
            try {
                long cursor = api.search(table, "*", "");
                try {
                    long fetched = api.fetchRow(cursor);
                    assertTrue(fetched != 0L);
                    try {
                        api.setNull(fetched, "name");
                        try {
                            api.update(table, fetched);
                            fail("Expected OpenFgdbException for update to NULL on NOT NULL column");
                        } catch (OpenFgdbException expected) {
                            assertNotNullViolation(expected, "name");
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
        } finally {
            api.close(db);
        }
    }

    @Test
    public void insertWithoutTIdAutoAssignsKey() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Assume.assumeTrue(isRealGdal(api));
        Path dbDir = Files.createTempDirectory("openfgdb4j-insert-autokey-").resolve("test.gdb");
        long db = api.create(dbDir.toString());
        try {
            api.execSql(db, "CREATE TABLE rgb(T_Id INTEGER PRIMARY KEY NOT NULL, iliCode VARCHAR)");
            api.execSql(db, "INSERT INTO rgb(iliCode) VALUES ('Rot')");
            api.execSql(db, "INSERT INTO rgb(iliCode) VALUES ('Blau')");
        } finally {
            api.close(db);
        }

        db = api.open(dbDir.toString());
        try {
            long table = api.openTable(db, "rgb");
            try {
                long cursor = api.search(table, "*", "");
                try {
                    List<Integer> ids = new ArrayList<Integer>();
                    while (true) {
                        long fetched = api.fetchRow(cursor);
                        if (fetched == 0L) {
                            break;
                        }
                        try {
                            assertFalse(api.rowIsNull(fetched, "T_Id"));
                            ids.add(api.rowGetInt32(fetched, "T_Id"));
                        } finally {
                            api.closeRow(fetched);
                        }
                    }
                    assertEquals(2, ids.size());
                    Collections.sort(ids);
                    assertEquals(Integer.valueOf(1), ids.get(0));
                    assertEquals(Integer.valueOf(2), ids.get(1));
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
    public void insertWithExplicitTIdStillWorks() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Assume.assumeTrue(isRealGdal(api));
        Path dbDir = Files.createTempDirectory("openfgdb4j-insert-explicit-key-").resolve("test.gdb");
        long db = api.create(dbDir.toString());
        try {
            api.execSql(db, "CREATE TABLE rgb(T_Id INTEGER PRIMARY KEY NOT NULL, iliCode VARCHAR)");
            api.execSql(db, "INSERT INTO rgb(T_Id, iliCode) VALUES (7, 'Rot')");
            api.execSql(db, "INSERT INTO rgb(iliCode) VALUES ('Blau')");
        } finally {
            api.close(db);
        }

        db = api.open(dbDir.toString());
        try {
            long table = api.openTable(db, "rgb");
            try {
                long cursor = api.search(table, "*", "");
                try {
                    Map<String, Integer> idsByCode = new HashMap<String, Integer>();
                    while (true) {
                        long fetched = api.fetchRow(cursor);
                        if (fetched == 0L) {
                            break;
                        }
                        try {
                            idsByCode.put(api.rowGetString(fetched, "iliCode"), api.rowGetInt32(fetched, "T_Id"));
                        } finally {
                            api.closeRow(fetched);
                        }
                    }
                    assertEquals(2, idsByCode.size());
                    assertEquals(Integer.valueOf(7), idsByCode.get("Rot"));
                    assertEquals(Integer.valueOf(8), idsByCode.get("Blau"));
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
    public void insertWithoutTIdOnTableWithoutKeyUnchanged() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Assume.assumeTrue(isRealGdal(api));
        Path dbDir = Files.createTempDirectory("openfgdb4j-insert-no-key-").resolve("test.gdb");
        long db = api.create(dbDir.toString());
        try {
            api.execSql(db, "CREATE TABLE t_plain(id INTEGER, iliCode VARCHAR)");
            api.execSql(db, "INSERT INTO t_plain(iliCode) VALUES ('Rot')");

            long table = api.openTable(db, "t_plain");
            try {
                long cursor = api.search(table, "*", "");
                try {
                    long fetched = api.fetchRow(cursor);
                    assertTrue(fetched != 0L);
                    try {
                        assertTrue(api.rowIsNull(fetched, "id"));
                        assertEquals("Rot", api.rowGetString(fetched, "iliCode"));
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
                        assertEquals(1, wkbType(geom));
                        byte[] blobView = api.rowGetBlob(fetched, "shape");
                        assertTrue(blobView != null && blobView.length > 0);
                        assertEquals(1, wkbType(blobView));
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
    public void geometryMultipointColumnIsRegisteredAndReadable() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Path dbDir = Files.createTempDirectory("openfgdb4j-geom-multipoint-").resolve("test.gdb");
        long db = api.create(dbDir.toString());
        try {
            api.execSql(db, "CREATE TABLE t_multipoint(id INTEGER, shape OFGDB_GEOMETRY(MULTIPOINT,2056,2) NOT NULL)");
            long table = api.openTable(db, "t_multipoint");
            try {
                long row = api.createRow(table);
                try {
                    api.setInt32(row, "id", 22);
                    api.setGeometry(row, multiPointWkb(new double[][] {{2600000.5, 1200000.25}, {2600001.5, 1200001.25}}));
                    api.insert(table, row);
                } finally {
                    api.closeRow(row);
                }

                long cursor = api.search(table, "*", "");
                try {
                    long fetched = api.fetchRow(cursor);
                    try {
                        assertEquals(Integer.valueOf(22), api.rowGetInt32(fetched, "id"));
                        byte[] geom = api.rowGetGeometry(fetched);
                        assertTrue(geom != null && geom.length > 0);
                        assertEquals(4, wkbType(geom));
                        byte[] blobView = api.rowGetBlob(fetched, "shape");
                        assertTrue(blobView != null && blobView.length > 0);
                        assertEquals(4, wkbType(blobView));
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
    public void geometryMultilineColumnIsRegisteredAndReadable() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Path dbDir = Files.createTempDirectory("openfgdb4j-geom-multiline-").resolve("test.gdb");
        long db = api.create(dbDir.toString());
        try {
            api.execSql(db, "CREATE TABLE t_multiline(id INTEGER, shape OFGDB_GEOMETRY(MULTILINE,2056,2) NOT NULL)");
            long table = api.openTable(db, "t_multiline");
            try {
                long row = api.createRow(table);
                try {
                    api.setInt32(row, "id", 33);
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
                    try {
                        assertEquals(Integer.valueOf(33), api.rowGetInt32(fetched, "id"));
                        byte[] geom = api.rowGetGeometry(fetched);
                        assertTrue(geom != null && geom.length > 0);
                        assertEquals(5, wkbType(geom));
                        byte[] blobView = api.rowGetBlob(fetched, "shape");
                        assertTrue(blobView != null && blobView.length > 0);
                        assertEquals(5, wkbType(blobView));
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
    public void geometryMultipolygonColumnIsRegisteredAndReadable() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Path dbDir = Files.createTempDirectory("openfgdb4j-geom-multipolygon-").resolve("test.gdb");
        long db = api.create(dbDir.toString());
        try {
            api.execSql(db, "CREATE TABLE t_multipolygon(id INTEGER, shape OFGDB_GEOMETRY(MULTIPOLYGON,2056,2) NOT NULL)");
            long table = api.openTable(db, "t_multipolygon");
            try {
                long row = api.createRow(table);
                try {
                    api.setInt32(row, "id", 44);
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
                    try {
                        assertEquals(Integer.valueOf(44), api.rowGetInt32(fetched, "id"));
                        byte[] geom = api.rowGetGeometry(fetched);
                        assertTrue(geom != null && geom.length > 0);
                        assertEquals(6, wkbType(geom));
                        byte[] blobView = api.rowGetBlob(fetched, "shape");
                        assertTrue(blobView != null && blobView.length > 0);
                        assertEquals(6, wkbType(blobView));
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
    public void geometryCompoundcurveColumnIsRegisteredAndReadable() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Assume.assumeTrue(isRealGdal(api));
        Path dbDir = Files.createTempDirectory("openfgdb4j-geom-compoundcurve-").resolve("test.gdb");
        long db = api.create(dbDir.toString());
        try {
            api.execSql(db, "CREATE TABLE t_compoundcurve(id INTEGER, shape OFGDB_GEOMETRY(COMPOUNDCURVE,2056,2) NOT NULL)");
            long table = api.openTable(db, "t_compoundcurve");
            try {
                long row = api.createRow(table);
                try {
                    api.setInt32(row, "id", 55);
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
                    try {
                        assertEquals(Integer.valueOf(55), api.rowGetInt32(fetched, "id"));
                        byte[] geom = api.rowGetGeometry(fetched);
                        assertTrue(geom != null && geom.length > 0);
                        assertEquals(9, wkbType(geom));
                        byte[] blobView = api.rowGetBlob(fetched, "shape");
                        assertTrue(blobView != null && blobView.length > 0);
                        assertEquals(9, wkbType(blobView));
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
    public void geometryMulticurveColumnIsRegisteredAndReadable() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Assume.assumeTrue(isRealGdal(api));
        Path dbDir = Files.createTempDirectory("openfgdb4j-geom-multicurve-").resolve("test.gdb");
        long db = api.create(dbDir.toString());
        try {
            api.execSql(db, "CREATE TABLE t_multicurve(id INTEGER, shape OFGDB_GEOMETRY(MULTICURVE,2056,2) NOT NULL)");
            long table = api.openTable(db, "t_multicurve");
            try {
                long row = api.createRow(table);
                try {
                    api.setInt32(row, "id", 66);
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
                    try {
                        assertEquals(Integer.valueOf(66), api.rowGetInt32(fetched, "id"));
                        byte[] geom = api.rowGetGeometry(fetched);
                        assertTrue(geom != null && geom.length > 0);
                        assertEquals(11, wkbType(geom));
                        byte[] blobView = api.rowGetBlob(fetched, "shape");
                        assertTrue(blobView != null && blobView.length > 0);
                        assertEquals(11, wkbType(blobView));
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
    public void geometryCurvepolygonColumnIsRegisteredAndReadable() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Assume.assumeTrue(isRealGdal(api));
        Path dbDir = Files.createTempDirectory("openfgdb4j-geom-curvepolygon-").resolve("test.gdb");
        long db = api.create(dbDir.toString());
        try {
            api.execSql(db, "CREATE TABLE t_curvepolygon(id INTEGER, shape OFGDB_GEOMETRY(CURVEPOLYGON,2056,2) NOT NULL)");
            long table = api.openTable(db, "t_curvepolygon");
            try {
                long row = api.createRow(table);
                try {
                    api.setInt32(row, "id", 77);
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
                    try {
                        assertEquals(Integer.valueOf(77), api.rowGetInt32(fetched, "id"));
                        byte[] geom = api.rowGetGeometry(fetched);
                        assertTrue(geom != null && geom.length > 0);
                        assertEquals(10, wkbType(geom));
                        assertWkbContainsExpectedCoords(geom, new double[][] {
                                {2600000.0, 1200000.0},
                                {2600002.0, 1200002.0},
                                {2600004.0, 1200000.0}
                        });
                        byte[] blobView = api.rowGetBlob(fetched, "shape");
                        assertTrue(blobView != null && blobView.length > 0);
                        assertEquals(10, wkbType(blobView));
                        assertWkbContainsExpectedCoords(blobView, new double[][] {
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
        } finally {
            api.close(db);
        }
    }

    @Test
    public void geometryMultisurfaceColumnIsRegisteredAndReadable() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Assume.assumeTrue(isRealGdal(api));
        Path dbDir = Files.createTempDirectory("openfgdb4j-geom-multisurface-").resolve("test.gdb");
        long db = api.create(dbDir.toString());
        try {
            api.execSql(db, "CREATE TABLE t_multisurface(id INTEGER, shape OFGDB_GEOMETRY(MULTISURFACE,2056,2) NOT NULL)");
            long table = api.openTable(db, "t_multisurface");
            try {
                long row = api.createRow(table);
                try {
                    api.setInt32(row, "id", 88);
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
                    try {
                        assertEquals(Integer.valueOf(88), api.rowGetInt32(fetched, "id"));
                        byte[] geom = api.rowGetGeometry(fetched);
                        assertTrue(geom != null && geom.length > 0);
                        assertEquals(12, wkbType(geom));
                        assertWkbContainsExpectedCoords(geom, new double[][] {
                                {2600000.0, 1200000.0},
                                {2600002.0, 1200002.0},
                                {2600004.0, 1200000.0}
                        });
                        byte[] blobView = api.rowGetBlob(fetched, "shape");
                        assertTrue(blobView != null && blobView.length > 0);
                        assertEquals(12, wkbType(blobView));
                        assertWkbContainsExpectedCoords(blobView, new double[][] {
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
        } finally {
            api.close(db);
        }
    }

    @Test
    public void lineRejectsMultilineOnInsert() throws Exception {
        assertInsertTypeMismatch("CREATE TABLE t_line(id INTEGER, shape OFGDB_GEOMETRY(LINE,2056,2) NOT NULL)", "t_line",
                multiLineStringWkb(new double[][][] {{{2600000.0, 1200000.0}, {2600002.0, 1200001.0}}}), "expected LINE",
                "got MULTILINE");
    }

    @Test
    public void multilineRejectsLineOnInsert() throws Exception {
        assertInsertTypeMismatch("CREATE TABLE t_multiline(id INTEGER, shape OFGDB_GEOMETRY(MULTILINE,2056,2) NOT NULL)", "t_multiline",
                lineStringWkb(new double[][] {{2600000.0, 1200000.0}, {2600002.0, 1200001.0}}), "expected MULTILINE",
                "got LINE");
    }

    @Test
    public void polygonRejectsMultipolygonOnInsert() throws Exception {
        assertInsertTypeMismatch("CREATE TABLE t_polygon(id INTEGER, shape OFGDB_GEOMETRY(POLYGON,2056,2) NOT NULL)", "t_polygon",
                multiPolygonWkb(new double[][][][] {
                        {
                                {{2600000.0, 1200000.0}, {2600010.0, 1200000.0}, {2600010.0, 1200010.0}, {2600000.0, 1200010.0}, {2600000.0, 1200000.0}}
                        }
                }), "expected POLYGON", "got MULTIPOLYGON");
    }

    @Test
    public void multipolygonRejectsPolygonOnInsert() throws Exception {
        assertInsertTypeMismatch("CREATE TABLE t_multipolygon(id INTEGER, shape OFGDB_GEOMETRY(MULTIPOLYGON,2056,2) NOT NULL)", "t_multipolygon",
                polygonWkb(new double[][][] {
                        {{2600000.0, 1200000.0}, {2600010.0, 1200000.0}, {2600010.0, 1200010.0}, {2600000.0, 1200010.0}, {2600000.0, 1200000.0}}
                }), "expected MULTIPOLYGON", "got POLYGON");
    }

    @Test
    public void lineRejectsCompoundcurveOnInsert() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Assume.assumeTrue(isRealGdal(api));
        assertInsertTypeMismatch("CREATE TABLE t_line_curve(id INTEGER, shape OFGDB_GEOMETRY(LINE,2056,2) NOT NULL)", "t_line_curve",
                compoundCurveWkb(new double[][][] {
                        {{2600000.0, 1200000.0}, {2600002.0, 1200001.0}, {2600004.0, 1200002.0}}
                }), "expected LINE", "got COMPOUNDCURVE");
    }

    @Test
    public void compoundcurveRejectsLinestringOnInsert() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Assume.assumeTrue(isRealGdal(api));
        assertInsertTypeMismatch(
                "CREATE TABLE t_compoundcurve_line(id INTEGER, shape OFGDB_GEOMETRY(COMPOUNDCURVE,2056,2) NOT NULL)",
                "t_compoundcurve_line",
                lineStringWkb(new double[][] {{2600000.0, 1200000.0}, {2600002.0, 1200001.0}}),
                "expected COMPOUNDCURVE",
                "got LINE");
    }

    @Test
    public void polygonRejectsCurvepolygonOnInsert() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Assume.assumeTrue(isRealGdal(api));
        assertInsertTypeMismatch("CREATE TABLE t_polygon_curve(id INTEGER, shape OFGDB_GEOMETRY(POLYGON,2056,2) NOT NULL)", "t_polygon_curve",
                curvePolygonWkb(new byte[][] {
                        lineStringWkb(new double[][] {
                                {2600000.0, 1200000.0},
                                {2600010.0, 1200000.0},
                                {2600010.0, 1200010.0},
                                {2600000.0, 1200010.0},
                                {2600000.0, 1200000.0}
                        })
                }), "expected POLYGON", "got CURVEPOLYGON");
    }

    @Test
    public void curvepolygonRejectsPolygonOnInsert() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Assume.assumeTrue(isRealGdal(api));
        assertInsertTypeMismatch(
                "CREATE TABLE t_curvepolygon_polygon(id INTEGER, shape OFGDB_GEOMETRY(CURVEPOLYGON,2056,2) NOT NULL)",
                "t_curvepolygon_polygon",
                polygonWkb(new double[][][] {
                        {{2600000.0, 1200000.0}, {2600010.0, 1200000.0}, {2600010.0, 1200010.0}, {2600000.0, 1200010.0}, {2600000.0, 1200000.0}}
                }),
                "expected CURVEPOLYGON",
                "got POLYGON");
    }

    @Test
    public void invalidOfgdbGeometryDefinitionFailsFast() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Path dbDir = Files.createTempDirectory("openfgdb4j-invalid-geometry-").resolve("test.gdb");
        long db = api.create(dbDir.toString());
        try {
            try {
                api.execSql(db, "CREATE TABLE t_bad(id INTEGER, shape OFGDB_GEOMETRY(MULTICOORD,2056,2) NOT NULL)");
                fail("Expected OpenFgdbException for invalid OFGDB_GEOMETRY definition");
            } catch (OpenFgdbException expected) {
                assertEquals(OpenFgdb.OFGDB_ERR_INVALID_ARG, expected.getErrorCode());
                assertTrue(expected.getMessage().contains("OFGDB_GEOMETRY"));
            }
        } finally {
            api.close(db);
        }
    }

    @Test
    public void geometryContractSurvivesReopenLine() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Path dbDir = Files.createTempDirectory("openfgdb4j-reopen-line-").resolve("test.gdb");
        long dbCreate = api.create(dbDir.toString());
        try {
            api.execSql(dbCreate, "CREATE TABLE t_reopen_line(id INTEGER, geom_decl OFGDB_GEOMETRY(LINE,2056,2) NOT NULL)");
            long table = api.openTable(dbCreate, "t_reopen_line");
            try {
                long row = api.createRow(table);
                try {
                    api.setInt32(row, "id", 1);
                    api.setGeometry(row, lineStringWkb(new double[][] {{2600000.0, 1200000.0}, {2600002.0, 1200001.0}}));
                    api.insert(table, row);
                } finally {
                    api.closeRow(row);
                }
            } finally {
                api.closeTable(dbCreate, table);
            }
        } finally {
            api.close(dbCreate);
        }

        long dbOpen = api.open(dbDir.toString());
        try {
            long table = api.openTable(dbOpen, "t_reopen_line");
            try {
                long cursor = api.search(table, "*", "");
                try {
                    long fetched = api.fetchRow(cursor);
                    try {
                        assertEquals(Integer.valueOf(1), api.rowGetInt32(fetched, "id"));
                        assertFalse(api.rowIsNull(fetched, "geom_decl"));
                        byte[] blobView = api.rowGetBlob(fetched, "geom_decl");
                        assertTrue(blobView != null && blobView.length > 0);
                        assertEquals(2, wkbType(blobView));
                    } finally {
                        api.closeRow(fetched);
                    }
                } finally {
                    api.closeCursor(cursor);
                }
            } finally {
                api.closeTable(dbOpen, table);
            }
        } finally {
            api.close(dbOpen);
        }
    }

    @Test
    public void geometryContractSurvivesReopenCurvepolygon() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Assume.assumeTrue(isRealGdal(api));
        Path dbDir = Files.createTempDirectory("openfgdb4j-reopen-curvepolygon-").resolve("test.gdb");
        long dbCreate = api.create(dbDir.toString());
        try {
            api.execSql(dbCreate, "CREATE TABLE t_reopen_curvepolygon(id INTEGER, geom_decl OFGDB_GEOMETRY(CURVEPOLYGON,2056,2) NOT NULL)");
            long table = api.openTable(dbCreate, "t_reopen_curvepolygon");
            try {
                long row = api.createRow(table);
                try {
                    api.setInt32(row, "id", 1);
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
            } finally {
                api.closeTable(dbCreate, table);
            }
        } finally {
            api.close(dbCreate);
        }

        long dbOpen = api.open(dbDir.toString());
        try {
            long table = api.openTable(dbOpen, "t_reopen_curvepolygon");
            try {
                long cursor = api.search(table, "*", "");
                try {
                    long fetched = api.fetchRow(cursor);
                    try {
                        assertEquals(Integer.valueOf(1), api.rowGetInt32(fetched, "id"));
                        assertFalse(api.rowIsNull(fetched, "geom_decl"));
                        byte[] blobView = api.rowGetBlob(fetched, "geom_decl");
                        assertTrue(blobView != null && blobView.length > 0);
                        assertEquals(10, wkbType(blobView));
                        assertWkbContainsExpectedCoords(blobView, new double[][] {
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
                api.closeTable(dbOpen, table);
            }
        } finally {
            api.close(dbOpen);
        }
    }

    @Test
    public void geometryContractSurvivesReopenCompoundcurve() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Assume.assumeTrue(isRealGdal(api));
        Path dbDir = Files.createTempDirectory("openfgdb4j-reopen-compoundcurve-").resolve("test.gdb");
        long dbCreate = api.create(dbDir.toString());
        try {
            api.execSql(dbCreate,
                    "CREATE TABLE t_reopen_compoundcurve(id INTEGER, geom_decl OFGDB_GEOMETRY(COMPOUNDCURVE,2056,2) NOT NULL)");
            long table = api.openTable(dbCreate, "t_reopen_compoundcurve");
            try {
                long row = api.createRow(table);
                try {
                    api.setInt32(row, "id", 1);
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
            } finally {
                api.closeTable(dbCreate, table);
            }
        } finally {
            api.close(dbCreate);
        }

        long dbOpen = api.open(dbDir.toString());
        try {
            long table = api.openTable(dbOpen, "t_reopen_compoundcurve");
            try {
                long cursor = api.search(table, "*", "");
                try {
                    long fetched = api.fetchRow(cursor);
                    try {
                        assertEquals(Integer.valueOf(1), api.rowGetInt32(fetched, "id"));
                        assertFalse(api.rowIsNull(fetched, "geom_decl"));
                        byte[] blobView = api.rowGetBlob(fetched, "geom_decl");
                        assertTrue(blobView != null && blobView.length > 0);
                        assertEquals(9, wkbType(blobView));
                    } finally {
                        api.closeRow(fetched);
                    }
                } finally {
                    api.closeCursor(cursor);
                }
            } finally {
                api.closeTable(dbOpen, table);
            }
        } finally {
            api.close(dbOpen);
        }
    }

    @Test
    public void insertByDeclaredGeometryColumnNameWorks() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Path dbDir = Files.createTempDirectory("openfgdb4j-declared-insert-").resolve("test.gdb");
        long db = api.create(dbDir.toString());
        try {
            api.execSql(db, "CREATE TABLE t_declared_insert(id INTEGER, geom_decl OFGDB_GEOMETRY(POINT,2056,2) NOT NULL)");
            String literal = toByteLiteral(pointWkb(2600000.5, 1200000.25));
            api.execSql(db, "INSERT INTO t_declared_insert(id, geom_decl) VALUES (1, '" + literal + "')");

            long table = api.openTable(db, "t_declared_insert");
            try {
                long cursor = api.search(table, "*", "");
                try {
                    long fetched = api.fetchRow(cursor);
                    try {
                        assertEquals(Integer.valueOf(1), api.rowGetInt32(fetched, "id"));
                        byte[] blobView = api.rowGetBlob(fetched, "geom_decl");
                        assertTrue(blobView != null && blobView.length > 0);
                        assertEquals(1, wkbType(blobView));
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
    public void readByDeclaredGeometryColumnNameWorks() throws Exception {
        OpenFgdb api = new OpenFgdb();
        Path dbDir = Files.createTempDirectory("openfgdb4j-declared-read-").resolve("test.gdb");
        long db = api.create(dbDir.toString());
        try {
            api.execSql(db, "CREATE TABLE t_declared_read(id INTEGER, geom_decl OFGDB_GEOMETRY(POINT,2056,2) NOT NULL)");
            long table = api.openTable(db, "t_declared_read");
            try {
                long row = api.createRow(table);
                try {
                    api.setInt32(row, "id", 1);
                    api.setGeometry(row, pointWkb(2600000.5, 1200000.25));
                    api.insert(table, row);
                } finally {
                    api.closeRow(row);
                }

                long cursor = api.search(table, "*", "");
                try {
                    long fetched = api.fetchRow(cursor);
                    try {
                        assertEquals(Integer.valueOf(1), api.rowGetInt32(fetched, "id"));
                        assertFalse(api.rowIsNull(fetched, "geom_decl"));
                        byte[] blobView = api.rowGetBlob(fetched, "geom_decl");
                        assertTrue(blobView != null && blobView.length > 0);
                        assertEquals(1, wkbType(blobView));
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

    private static void assertInsertTypeMismatch(String createSql, String tableName, byte[] geometryWkb, String expectedToken,
            String gotToken) throws Exception {
        OpenFgdb api = new OpenFgdb();
        Path dbDir = Files.createTempDirectory("openfgdb4j-mismatch-").resolve("test.gdb");
        long db = api.create(dbDir.toString());
        try {
            api.execSql(db, createSql);
            long table = api.openTable(db, tableName);
            try {
                long row = api.createRow(table);
                try {
                    try {
                        api.setGeometry(row, geometryWkb);
                        fail("Expected OpenFgdbException for geometry type mismatch");
                    } catch (OpenFgdbException expected) {
                        assertEquals(OpenFgdb.OFGDB_ERR_INVALID_ARG, expected.getErrorCode());
                        assertTrue(expected.getMessage().contains(expectedToken));
                        assertTrue(expected.getMessage().contains(gotToken));
                    }
                } finally {
                    api.closeRow(row);
                }
            } finally {
                api.closeTable(db, table);
            }
        } finally {
            api.close(db);
        }
    }

    private static void assertNotNullViolation(OpenFgdbException expected, String columnName) {
        assertEquals(OpenFgdb.OFGDB_ERR_INVALID_ARG, expected.getErrorCode());
        String messageLower = expected.getMessage().toLowerCase();
        assertTrue(messageLower.contains("not null"));
        if (columnName != null && !columnName.isEmpty()) {
            assertTrue(messageLower.contains(columnName.toLowerCase()));
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

    private static String readDefinitionXml(OpenFgdb api, long db, String itemName) throws Exception {
        long tableHandle = api.openTable(db, "GDB_Items");
        try {
            long cursor = api.search(tableHandle, "Name,Definition", "");
            try {
                while (true) {
                    long row = api.fetchRow(cursor);
                    if (row == 0L) {
                        return null;
                    }
                    try {
                        String name = api.rowGetString(row, "Name");
                        if (name != null && name.equalsIgnoreCase(itemName)) {
                            return api.rowGetString(row, "Definition");
                        }
                    } finally {
                        api.closeRow(row);
                    }
                }
            } finally {
                api.closeCursor(cursor);
            }
        } finally {
            api.closeTable(db, tableHandle);
        }
    }

    private static Integer findFieldLength(String definitionXml, String fieldName) throws Exception {
        Document document = DocumentBuilderFactory.newInstance()
                .newDocumentBuilder()
                .parse(new InputSource(new StringReader(definitionXml)));
        NodeList allNodes = document.getElementsByTagName("*");
        for (int i = 0; i < allNodes.getLength(); i++) {
            Node node = allNodes.item(i);
            if (!(node instanceof Element) || !nodeNameMatches(node, "GPFieldInfoEx")) {
                continue;
            }
            Element fieldNode = (Element) node;
            String currentFieldName = childTagText(fieldNode, "Name");
            if (currentFieldName == null || !currentFieldName.equalsIgnoreCase(fieldName)) {
                continue;
            }
            String lengthText = childTagText(fieldNode, "Length");
            if (lengthText == null || lengthText.isEmpty()) {
                return null;
            }
            return Integer.valueOf(Integer.parseInt(lengthText));
        }
        return null;
    }

    private static String childTagText(Element parent, String tagName) {
        NodeList children = parent.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (child instanceof Element && nodeNameMatches(child, tagName)) {
                String text = child.getTextContent();
                return text != null ? text.trim() : null;
            }
        }
        return null;
    }

    private static boolean nodeNameMatches(Node node, String localTagName) {
        if (node == null || localTagName == null) {
            return false;
        }
        String name = node.getNodeName();
        return name != null && name.endsWith(localTagName);
    }

    private static boolean isRealGdal(OpenFgdb api) throws Exception {
        return api.getRuntimeInfo().contains("impl=real_gdal");
    }

    private static String toByteLiteral(byte[] value) {
        return "__OFGDB_BYTES_B64__:" + Base64.getEncoder().encodeToString(value);
    }

    private static byte[] pointWkb(double x, double y) {
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 8 + 8).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1); // little endian
        buffer.putInt(1);     // WKB Point
        buffer.putDouble(x);
        buffer.putDouble(y);
        return buffer.array();
    }

    private static byte[] multiPointWkb(double[][] points) {
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 4 + points.length * (1 + 4 + 8 + 8)).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1); // little endian
        buffer.putInt(4);     // WKB MultiPoint
        buffer.putInt(points.length);
        for (double[] point : points) {
            buffer.put((byte) 1); // little endian
            buffer.putInt(1);     // WKB Point
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
            assertTrue("missing coordinate pair " + coord[0] + "," + coord[1], containsCoordinatePairLe(wkb, coord[0], coord[1]));
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
}
