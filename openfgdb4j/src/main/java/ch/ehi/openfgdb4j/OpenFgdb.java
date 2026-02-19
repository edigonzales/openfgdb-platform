package ch.ehi.openfgdb4j;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class OpenFgdb {
    private static final Linker LINKER = Linker.nativeLinker();

    public static final int OFGDB_OK = 0;
    public static final int OFGDB_ERR_INVALID_ARG = 1;
    public static final int OFGDB_ERR_NOT_FOUND = 2;
    public static final int OFGDB_ERR_INTERNAL = 3;
    public static final int OFGDB_ERR_ALREADY_EXISTS = 4;

    private final SymbolLookup symbols;

    public OpenFgdb() {
        NativeLoader.load();
        this.symbols = SymbolLookup.loaderLookup();
    }

    public DbSession openSession(String path, boolean createIfMissing) throws OpenFgdbException {
        long handle = createIfMissing ? create(path) : open(path);
        return new DbSession(this, handle);
    }

    public long open(String path) throws OpenFgdbException {
        return openLike("ofgdb_open", path);
    }

    public long create(String path) throws OpenFgdbException {
        return openLike("ofgdb_create", path);
    }

    public void close(long dbHandle) throws OpenFgdbException {
        MethodHandle mh = downcall("ofgdb_close", FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG));
        int rc = invokeInt(mh, dbHandle);
        checkRc("ofgdb_close", rc);
    }

    public void execSql(long dbHandle, String sql) throws OpenFgdbException {
        MethodHandle mh = downcall("ofgdb_exec_sql", FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment sqlStr = arena.allocateFrom(sql);
            int rc = invokeInt(mh, dbHandle, sqlStr);
            checkRc("ofgdb_exec_sql", rc);
        }
    }

    public long openTable(long dbHandle, String tableName) throws OpenFgdbException {
        MethodHandle mh = downcall(
                "ofgdb_open_table",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment table = arena.allocateFrom(tableName);
            MemorySegment outHandle = arena.allocate(ValueLayout.JAVA_LONG);
            int rc = invokeInt(mh, dbHandle, table, outHandle);
            checkRc("ofgdb_open_table", rc);
            return outHandle.get(ValueLayout.JAVA_LONG, 0L);
        }
    }

    public void closeTable(long dbHandle, long tableHandle) throws OpenFgdbException {
        MethodHandle mh = downcall(
                "ofgdb_close_table",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG));
        int rc = invokeInt(mh, dbHandle, tableHandle);
        checkRc("ofgdb_close_table", rc);
    }

    public List<String> getFieldNames(long tableHandle) throws OpenFgdbException {
        long fieldInfo = 0L;
        try {
            MethodHandle openMh = downcall(
                    "ofgdb_get_field_info",
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
            MethodHandle countMh = downcall(
                    "ofgdb_field_info_count",
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
            MethodHandle nameMh = downcall(
                    "ofgdb_field_info_name",
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT, ValueLayout.ADDRESS));
            MethodHandle closeMh = downcall("ofgdb_close_field_info", FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG));
            MethodHandle freeMh = downcall("ofgdb_free_string", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

            try (Arena arena = Arena.ofConfined()) {
                MemorySegment infoOut = arena.allocate(ValueLayout.JAVA_LONG);
                int openRc = invokeInt(openMh, tableHandle, infoOut);
                checkRc("ofgdb_get_field_info", openRc);
                fieldInfo = infoOut.get(ValueLayout.JAVA_LONG, 0L);

                MemorySegment countOut = arena.allocate(ValueLayout.JAVA_INT);
                int countRc = invokeInt(countMh, fieldInfo, countOut);
                checkRc("ofgdb_field_info_count", countRc);
                int count = countOut.get(ValueLayout.JAVA_INT, 0L);

                List<String> names = new ArrayList<String>(count);
                for (int i = 0; i < count; i++) {
                    MemorySegment nameOut = arena.allocate(ValueLayout.ADDRESS);
                    int nameRc = invokeInt(nameMh, fieldInfo, i, nameOut);
                    checkRc("ofgdb_field_info_name", nameRc);
                    MemorySegment rawPtr = nameOut.get(ValueLayout.ADDRESS, 0L);
                    if (rawPtr == null || rawPtr.address() == 0L) {
                        names.add("");
                    } else {
                        names.add(readCString(rawPtr));
                        invokeVoid(freeMh, rawPtr);
                    }
                }
                int closeRc = invokeInt(closeMh, fieldInfo);
                checkRc("ofgdb_close_field_info", closeRc);
                fieldInfo = 0L;
                return names;
            }
        } finally {
            if (fieldInfo != 0L) {
                try {
                    MethodHandle closeMh = downcall("ofgdb_close_field_info", FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG));
                    invokeInt(closeMh, fieldInfo);
                } catch (OpenFgdbException ignore) {
                }
            }
        }
    }

    public long search(long tableHandle, String fields, String whereClause) throws OpenFgdbException {
        MethodHandle mh = downcall(
                "ofgdb_search",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outHandle = arena.allocate(ValueLayout.JAVA_LONG);
            int rc = invokeInt(
                    mh,
                    tableHandle,
                    arena.allocateFrom(fields != null ? fields : "*"),
                    arena.allocateFrom(whereClause != null ? whereClause : ""),
                    outHandle);
            checkRc("ofgdb_search", rc);
            return outHandle.get(ValueLayout.JAVA_LONG, 0L);
        }
    }

    public long fetchRow(long cursorHandle) throws OpenFgdbException {
        MethodHandle mh = downcall("ofgdb_fetch_row", FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outHandle = arena.allocate(ValueLayout.JAVA_LONG);
            int rc = invokeInt(mh, cursorHandle, outHandle);
            checkRc("ofgdb_fetch_row", rc);
            return outHandle.get(ValueLayout.JAVA_LONG, 0L);
        }
    }

    public void closeCursor(long cursorHandle) throws OpenFgdbException {
        MethodHandle mh = downcall("ofgdb_close_cursor", FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG));
        int rc = invokeInt(mh, cursorHandle);
        checkRc("ofgdb_close_cursor", rc);
    }

    public void closeRow(long rowHandle) throws OpenFgdbException {
        MethodHandle mh = downcall("ofgdb_close_row", FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG));
        int rc = invokeInt(mh, rowHandle);
        checkRc("ofgdb_close_row", rc);
    }

    public long createRow(long tableHandle) throws OpenFgdbException {
        MethodHandle mh = downcall(
                "ofgdb_create_row",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outHandle = arena.allocate(ValueLayout.JAVA_LONG);
            int rc = invokeInt(mh, tableHandle, outHandle);
            checkRc("ofgdb_create_row", rc);
            return outHandle.get(ValueLayout.JAVA_LONG, 0L);
        }
    }

    public void insert(long tableHandle, long rowHandle) throws OpenFgdbException {
        MethodHandle mh = downcall(
                "ofgdb_insert",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG));
        int rc = invokeInt(mh, tableHandle, rowHandle);
        checkRc("ofgdb_insert", rc);
    }

    public void update(long tableHandle, long rowHandle) throws OpenFgdbException {
        MethodHandle mh = downcall(
                "ofgdb_update",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG));
        int rc = invokeInt(mh, tableHandle, rowHandle);
        checkRc("ofgdb_update", rc);
    }

    public void setString(long rowHandle, String columnName, String value) throws OpenFgdbException {
        MethodHandle mh = downcall(
                "ofgdb_set_string",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
        try (Arena arena = Arena.ofConfined()) {
            int rc = invokeInt(mh, rowHandle, arena.allocateFrom(columnName), arena.allocateFrom(value != null ? value : ""));
            checkRc("ofgdb_set_string", rc);
        }
    }

    public void setInt32(long rowHandle, String columnName, int value) throws OpenFgdbException {
        MethodHandle mh = downcall(
                "ofgdb_set_int32",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_INT));
        try (Arena arena = Arena.ofConfined()) {
            int rc = invokeInt(mh, rowHandle, arena.allocateFrom(columnName), value);
            checkRc("ofgdb_set_int32", rc);
        }
    }

    public void setDouble(long rowHandle, String columnName, double value) throws OpenFgdbException {
        MethodHandle mh = downcall(
                "ofgdb_set_double",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_DOUBLE));
        try (Arena arena = Arena.ofConfined()) {
            int rc = invokeInt(mh, rowHandle, arena.allocateFrom(columnName), value);
            checkRc("ofgdb_set_double", rc);
        }
    }

    public void setBlob(long rowHandle, String columnName, byte[] value) throws OpenFgdbException {
        MethodHandle mh = downcall(
                "ofgdb_set_blob",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT));
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment data = value != null && value.length > 0 ? arena.allocate(value.length) : MemorySegment.NULL;
            if (value != null && value.length > 0) {
                for (int i = 0; i < value.length; i++) {
                    data.set(ValueLayout.JAVA_BYTE, i, value[i]);
                }
            }
            int rc = invokeInt(mh, rowHandle, arena.allocateFrom(columnName), data, value != null ? value.length : 0);
            checkRc("ofgdb_set_blob", rc);
        }
    }

    public void setGeometry(long rowHandle, byte[] wkb) throws OpenFgdbException {
        MethodHandle mh = downcall(
                "ofgdb_set_geometry",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_INT));
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment data = wkb != null && wkb.length > 0 ? arena.allocate(wkb.length) : MemorySegment.NULL;
            if (wkb != null && wkb.length > 0) {
                for (int i = 0; i < wkb.length; i++) {
                    data.set(ValueLayout.JAVA_BYTE, i, wkb[i]);
                }
            }
            int rc = invokeInt(mh, rowHandle, data, wkb != null ? wkb.length : 0);
            checkRc("ofgdb_set_geometry", rc);
        }
    }

    public void setNull(long rowHandle, String columnName) throws OpenFgdbException {
        MethodHandle mh = downcall(
                "ofgdb_set_null",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
        try (Arena arena = Arena.ofConfined()) {
            int rc = invokeInt(mh, rowHandle, arena.allocateFrom(columnName));
            checkRc("ofgdb_set_null", rc);
        }
    }

    public String rowGetString(long rowHandle, String columnName) throws OpenFgdbException {
        MethodHandle mh = downcall(
                "ofgdb_row_get_string",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
        MethodHandle free = downcall("ofgdb_free_string", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outPtr = arena.allocate(ValueLayout.ADDRESS);
            int rc = invokeInt(mh, rowHandle, arena.allocateFrom(columnName), outPtr);
            checkRc("ofgdb_row_get_string", rc);
            MemorySegment rawPtr = outPtr.get(ValueLayout.ADDRESS, 0L);
            if (rawPtr == null || rawPtr.address() == 0L) {
                return null;
            }
            String value = readCString(rawPtr);
            invokeVoid(free, rawPtr);
            return value;
        }
    }

    public boolean rowIsNull(long rowHandle, String columnName) throws OpenFgdbException {
        MethodHandle mh = downcall(
                "ofgdb_row_is_null",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outFlag = arena.allocate(ValueLayout.JAVA_INT);
            int rc = invokeInt(mh, rowHandle, arena.allocateFrom(columnName), outFlag);
            checkRc("ofgdb_row_is_null", rc);
            return outFlag.get(ValueLayout.JAVA_INT, 0L) != 0;
        }
    }

    public Integer rowGetInt32(long rowHandle, String columnName) throws OpenFgdbException {
        MethodHandle mh = downcall(
                "ofgdb_row_get_int32",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outValue = arena.allocate(ValueLayout.JAVA_INT);
            int rc = invokeInt(mh, rowHandle, arena.allocateFrom(columnName), outValue);
            checkRc("ofgdb_row_get_int32", rc);
            if (rowIsNull(rowHandle, columnName)) {
                return null;
            }
            return Integer.valueOf(outValue.get(ValueLayout.JAVA_INT, 0L));
        }
    }

    public Double rowGetDouble(long rowHandle, String columnName) throws OpenFgdbException {
        MethodHandle mh = downcall(
                "ofgdb_row_get_double",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outValue = arena.allocate(ValueLayout.JAVA_DOUBLE);
            int rc = invokeInt(mh, rowHandle, arena.allocateFrom(columnName), outValue);
            checkRc("ofgdb_row_get_double", rc);
            if (rowIsNull(rowHandle, columnName)) {
                return null;
            }
            return Double.valueOf(outValue.get(ValueLayout.JAVA_DOUBLE, 0L));
        }
    }

    public byte[] rowGetBlob(long rowHandle, String columnName) throws OpenFgdbException {
        MethodHandle mh = downcall(
                "ofgdb_row_get_blob",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
        MethodHandle free = downcall("ofgdb_free_string", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outPtr = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment outSize = arena.allocate(ValueLayout.JAVA_INT);
            int rc = invokeInt(mh, rowHandle, arena.allocateFrom(columnName), outPtr, outSize);
            checkRc("ofgdb_row_get_blob", rc);
            int size = outSize.get(ValueLayout.JAVA_INT, 0L);
            if (size <= 0) {
                return rowIsNull(rowHandle, columnName) ? null : new byte[0];
            }
            MemorySegment dataPtr = outPtr.get(ValueLayout.ADDRESS, 0L);
            if (dataPtr == null || dataPtr.address() == 0L) {
                return new byte[0];
            }
            MemorySegment data = dataPtr.reinterpret(size);
            byte[] out = new byte[size];
            for (int i = 0; i < size; i++) {
                out[i] = data.get(ValueLayout.JAVA_BYTE, i);
            }
            invokeVoid(free, dataPtr);
            return out;
        }
    }

    public byte[] rowGetGeometry(long rowHandle) throws OpenFgdbException {
        MethodHandle mh = downcall(
                "ofgdb_row_get_geometry",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
        MethodHandle free = downcall("ofgdb_free_string", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outPtr = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment outSize = arena.allocate(ValueLayout.JAVA_INT);
            int rc = invokeInt(mh, rowHandle, outPtr, outSize);
            checkRc("ofgdb_row_get_geometry", rc);
            int size = outSize.get(ValueLayout.JAVA_INT, 0L);
            if (size <= 0) {
                return null;
            }
            MemorySegment dataPtr = outPtr.get(ValueLayout.ADDRESS, 0L);
            if (dataPtr == null || dataPtr.address() == 0L) {
                return null;
            }
            MemorySegment data = dataPtr.reinterpret(size);
            byte[] out = new byte[size];
            for (int i = 0; i < size; i++) {
                out[i] = data.get(ValueLayout.JAVA_BYTE, i);
            }
            invokeVoid(free, dataPtr);
            return out;
        }
    }

    public List<String> listDomains(long dbHandle) throws OpenFgdbException {
        String raw = readStringOut("ofgdb_list_domains_text", dbHandle);
        return splitLines(raw);
    }

    public List<String> listRelationships(long dbHandle) throws OpenFgdbException {
        String raw = readStringOut("ofgdb_list_relationships_text", dbHandle);
        return splitLines(raw);
    }

    public List<String> listTableNames(long dbHandle) throws OpenFgdbException {
        String raw = readStringOut("ofgdb_list_tables_text", dbHandle);
        return splitLines(raw);
    }

    public String getRuntimeInfo() throws OpenFgdbException {
        return readStringOutNoDbHandle("ofgdb_list_runtime_info_text");
    }

    public void createCodedDomain(long dbHandle, String domainName, String fieldType) throws OpenFgdbException {
        MethodHandle mh = downcall(
                "ofgdb_create_coded_domain",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment domain = arena.allocateFrom(domainName);
            MemorySegment type = arena.allocateFrom(fieldType);
            int rc = invokeInt(mh, dbHandle, domain, type);
            checkRc("ofgdb_create_coded_domain", rc);
        }
    }

    public void addCodedValue(long dbHandle, String domainName, String code, String label) throws OpenFgdbException {
        MethodHandle mh = downcall(
                "ofgdb_add_coded_value",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment domain = arena.allocateFrom(domainName);
            MemorySegment c = arena.allocateFrom(code);
            MemorySegment l = arena.allocateFrom(label != null ? label : code);
            int rc = invokeInt(mh, dbHandle, domain, c, l);
            checkRc("ofgdb_add_coded_value", rc);
        }
    }

    public void assignDomainToField(long dbHandle, String tableName, String columnName, String domainName) throws OpenFgdbException {
        MethodHandle mh = downcall(
                "ofgdb_assign_domain_to_field",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment table = arena.allocateFrom(tableName);
            MemorySegment col = arena.allocateFrom(columnName);
            MemorySegment domain = arena.allocateFrom(domainName);
            int rc = invokeInt(mh, dbHandle, table, col, domain);
            checkRc("ofgdb_assign_domain_to_field", rc);
        }
    }

    public void createRelationshipClass(
            long dbHandle,
            String name,
            String originTable,
            String destinationTable,
            String originPk,
            String originFk,
            String forwardLabel,
            String backwardLabel,
            String cardinality,
            boolean composite,
            boolean attributed) throws OpenFgdbException {
        MethodHandle mh = downcall(
                "ofgdb_create_relationship_class",
                FunctionDescriptor.of(
                        ValueLayout.JAVA_INT,
                        ValueLayout.JAVA_LONG,
                        ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS,
                        ValueLayout.JAVA_INT,
                        ValueLayout.JAVA_INT));
        try (Arena arena = Arena.ofConfined()) {
            int rc = invokeInt(
                    mh,
                    dbHandle,
                    arena.allocateFrom(name),
                    arena.allocateFrom(originTable),
                    arena.allocateFrom(destinationTable),
                    arena.allocateFrom(originPk),
                    arena.allocateFrom(originFk),
                    arena.allocateFrom(forwardLabel != null ? forwardLabel : ""),
                    arena.allocateFrom(backwardLabel != null ? backwardLabel : ""),
                    arena.allocateFrom(cardinality),
                    composite ? 1 : 0,
                    attributed ? 1 : 0);
            checkRc("ofgdb_create_relationship_class", rc);
        }
    }

    private long openLike(String symbol, String path) throws OpenFgdbException {
        MethodHandle mh = downcall(symbol, FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment pathStr = arena.allocateFrom(path);
            MemorySegment outHandle = arena.allocate(ValueLayout.JAVA_LONG);
            int rc = invokeInt(mh, pathStr, outHandle);
            checkRc(symbol, rc);
            return outHandle.get(ValueLayout.JAVA_LONG, 0L);
        }
    }

    private String readStringOut(String symbol, long dbHandle) throws OpenFgdbException {
        MethodHandle mh = downcall(symbol, FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
        MethodHandle free = downcall("ofgdb_free_string", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outPtr = arena.allocate(ValueLayout.ADDRESS);
            int rc = invokeInt(mh, dbHandle, outPtr);
            checkRc(symbol, rc);
            MemorySegment rawPtr = outPtr.get(ValueLayout.ADDRESS, 0L);
            if (rawPtr == null || rawPtr.address() == 0L) {
                return "";
            }
            String value = readCString(rawPtr);
            invokeVoid(free, rawPtr);
            return value;
        }
    }

    private String readStringOutNoDbHandle(String symbol) throws OpenFgdbException {
        MethodHandle mh = downcall(symbol, FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));
        MethodHandle free = downcall("ofgdb_free_string", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outPtr = arena.allocate(ValueLayout.ADDRESS);
            int rc = invokeInt(mh, outPtr);
            checkRc(symbol, rc);
            MemorySegment rawPtr = outPtr.get(ValueLayout.ADDRESS, 0L);
            if (rawPtr == null || rawPtr.address() == 0L) {
                return "";
            }
            String value = readCString(rawPtr);
            invokeVoid(free, rawPtr);
            return value;
        }
    }

    private static List<String> splitLines(String raw) {
        if (raw == null || raw.isEmpty()) {
            return Collections.emptyList();
        }
        String[] lines = raw.split("\\r?\\n");
        List<String> values = new ArrayList<String>(lines.length);
        for (String line : lines) {
            if (line != null && !line.isEmpty()) {
                values.add(line);
            }
        }
        return values;
    }

    private void checkRc(String fn, int rc) throws OpenFgdbException {
        if (rc == OFGDB_OK) {
            return;
        }
        throw new OpenFgdbException(fn + " failed with " + errorCodeName(rc) + " (" + rc + "): " + readLastError(), rc);
    }

    private static String errorCodeName(int rc) {
        switch (rc) {
            case OFGDB_OK:
                return "OFGDB_OK";
            case OFGDB_ERR_INVALID_ARG:
                return "OFGDB_ERR_INVALID_ARG";
            case OFGDB_ERR_NOT_FOUND:
                return "OFGDB_ERR_NOT_FOUND";
            case OFGDB_ERR_INTERNAL:
                return "OFGDB_ERR_INTERNAL";
            case OFGDB_ERR_ALREADY_EXISTS:
                return "OFGDB_ERR_ALREADY_EXISTS";
            default:
                return "OFGDB_ERR_UNKNOWN";
        }
    }

    private String readLastError() {
        MethodHandle mh = downcall("ofgdb_last_error_message", FunctionDescriptor.of(ValueLayout.ADDRESS));
        try {
            MemorySegment seg = (MemorySegment) mh.invokeExact();
            if (seg == null || seg.address() == 0L) {
                return "<no native error message>";
            }
            return readCString(seg);
        } catch (Throwable e) {
            return "<failed to read native error: " + e.getMessage() + ">";
        }
    }

    private static String readCString(MemorySegment ptr) {
        return ptr.reinterpret(Long.MAX_VALUE).getString(0L);
    }

    private MethodHandle downcall(String symbol, FunctionDescriptor fd) {
        MemorySegment sym = symbols.find(symbol)
                .orElseThrow(() -> new IllegalStateException("Native symbol not found: " + symbol));
        return LINKER.downcallHandle(sym, fd);
    }

    private int invokeInt(MethodHandle mh, Object... args) throws OpenFgdbException {
        try {
            return (int) mh.invokeWithArguments(args);
        } catch (Throwable e) {
            throw new OpenFgdbException("Native invocation failed", e);
        }
    }

    private void invokeVoid(MethodHandle mh, Object... args) throws OpenFgdbException {
        try {
            mh.invokeWithArguments(args);
        } catch (Throwable e) {
            throw new OpenFgdbException("Native invocation failed", e);
        }
    }

    public static final class DbSession implements AutoCloseable {
        private final OpenFgdb api;
        private long handle;

        DbSession(OpenFgdb api, long handle) {
            this.api = api;
            this.handle = handle;
        }

        public long handle() {
            return handle;
        }

        @Override
        public void close() throws OpenFgdbException {
            if (handle == 0L) {
                return;
            }
            long current = handle;
            handle = 0L;
            api.close(current);
        }
    }
}
