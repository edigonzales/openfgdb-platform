#include "openfgdb_native_adapter.hpp"
#include "openfgdb_c_api.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

std::string trim(const std::string& in) {
  size_t start = 0;
  while (start < in.size() && std::isspace(static_cast<unsigned char>(in[start])) != 0) {
    start++;
  }
  size_t end = in.size();
  while (end > start && std::isspace(static_cast<unsigned char>(in[end - 1])) != 0) {
    end--;
  }
  return in.substr(start, end - start);
}

std::string upper(const std::string& in) {
  std::string out(in);
  std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) {
    return static_cast<char>(std::toupper(c));
  });
  return out;
}

std::vector<std::string> split(const std::string& in, char sep) {
  std::vector<std::string> out;
  std::string cur;
  for (char c : in) {
    if (c == sep) {
      out.push_back(trim(cur));
      cur.clear();
    } else {
      cur.push_back(c);
    }
  }
  out.push_back(trim(cur));
  return out;
}

std::string unquote(std::string value) {
  value = trim(value);
  if (value.size() >= 2 && ((value.front() == '\'' && value.back() == '\'') || (value.front() == '"' && value.back() == '"'))) {
    std::string unquoted = value.substr(1, value.size() - 2);
    if (value.front() == '\'') {
      std::string decoded;
      decoded.reserve(unquoted.size());
      for (size_t i = 0; i < unquoted.size(); i++) {
        if (unquoted[i] == '\'' && i + 1 < unquoted.size() && unquoted[i + 1] == '\'') {
          decoded.push_back('\'');
          i++;
        } else {
          decoded.push_back(unquoted[i]);
        }
      }
      return decoded;
    }
    return unquoted;
  }
  return value;
}

std::vector<std::string> split_sql_values(const std::string& in) {
  std::vector<std::string> out;
  std::string cur;
  bool in_quote = false;
  for (size_t i = 0; i < in.size(); i++) {
    char c = in[i];
    if (c == '\'') {
      in_quote = !in_quote;
      cur.push_back(c);
      continue;
    }
    if (c == ',' && !in_quote) {
      out.push_back(trim(cur));
      cur.clear();
      continue;
    }
    cur.push_back(c);
  }
  out.push_back(trim(cur));
  return out;
}

char* dup_cstr(const std::string& value) {
  char* out = static_cast<char*>(std::malloc(value.size() + 1));
  if (out == nullptr) {
    return nullptr;
  }
  std::memcpy(out, value.c_str(), value.size() + 1);
  return out;
}

uint8_t* dup_bytes(const std::vector<uint8_t>& value) {
  if (value.empty()) {
    return nullptr;
  }
  uint8_t* out = static_cast<uint8_t*>(std::malloc(value.size()));
  if (out == nullptr) {
    return nullptr;
  }
  std::memcpy(out, value.data(), value.size());
  return out;
}

struct StoredValue {
  enum class Type {
    kNull,
    kString,
    kInt,
    kDouble,
    kBlob,
    kGeometry,
  };

  Type type = Type::kNull;
  std::string text;
  int32_t int_value = 0;
  double double_value = 0.0;
  std::vector<uint8_t> bytes;
};

StoredValue parse_sql_literal(const std::string& raw_value) {
  StoredValue v;
  std::string value = trim(raw_value);
  if (upper(value) == "NULL") {
    v.type = StoredValue::Type::kNull;
    return v;
  }
  if (!value.empty() && value.front() == '\'') {
    v.type = StoredValue::Type::kString;
    v.text = unquote(value);
    return v;
  }
  v.type = StoredValue::Type::kString;
  v.text = value;
  return v;
}

struct TableState {
  std::vector<std::string> columns;
  std::vector<std::unordered_map<std::string, StoredValue>> rows;
};

struct DomainState {
  std::string field_type;
  std::map<std::string, std::string> coded_values;
};

struct RelationshipState {
  std::string origin_table;
  std::string destination_table;
  std::string origin_pk;
  std::string origin_fk;
  std::string forward_label;
  std::string backward_label;
  std::string cardinality;
  bool composite = false;
  bool attributed = false;
};

struct DbState {
  std::string root_path;
  std::unordered_map<std::string, TableState> tables;
  std::unordered_map<std::string, DomainState> domains;
  std::set<std::string> assignments;
  std::unordered_map<std::string, RelationshipState> relationships;
};

struct TableHandle {
  uint64_t db_handle = 0;
  std::string table_name;
};

struct RowHandle {
  uint64_t db_handle = 0;
  std::string table_name;
  std::unordered_map<std::string, StoredValue> values;
};

struct CursorHandle {
  uint64_t db_handle = 0;
  std::vector<std::unordered_map<std::string, StoredValue>> rows;
  size_t index = 0;
};

struct FieldInfoHandle {
  uint64_t db_handle = 0;
  std::vector<std::string> columns;
};

}  // namespace

namespace openfgdb {

class NativeState {
 public:
  static NativeState& instance() {
    static NativeState state;
    return state;
  }

  std::mutex mutex;
  uint64_t next_handle = 1;
  std::unordered_map<uint64_t, std::shared_ptr<DbState>> dbs;
  std::unordered_map<std::string, std::shared_ptr<DbState>> path_dbs;
  std::unordered_map<uint64_t, TableHandle> tables;
  std::unordered_map<uint64_t, RowHandle> rows;
  std::unordered_map<uint64_t, CursorHandle> cursors;
  std::unordered_map<uint64_t, FieldInfoHandle> field_infos;

  std::string last_error;

  uint64_t allocate_handle() {
    return next_handle++;
  }
};

NativeAdapter::NativeAdapter() = default;

NativeAdapter& NativeAdapter::instance() {
  static NativeAdapter adapter;
  return adapter;
}

int NativeAdapter::fail(int code, const std::string& message) {
  NativeState::instance().last_error = message;
  return code;
}

static std::string normalize_db_path(const std::string& path) {
  std::error_code ec;
  std::filesystem::path fs_path(path);
  std::filesystem::path normalized = fs_path.lexically_normal();
  if (normalized.is_relative()) {
    std::filesystem::path absolute = std::filesystem::absolute(normalized, ec);
    if (!ec) {
      normalized = absolute.lexically_normal();
    }
  }
  return normalized.string();
}

static void rebuild_catalog_tables(DbState& db);

static StoredValue make_string_value(const std::string& value) {
  StoredValue v;
  v.type = StoredValue::Type::kString;
  v.text = value;
  return v;
}

static std::unordered_map<std::string, StoredValue> make_item_row(
    const std::string& name,
    const std::string& item_type_uuid,
    const std::string& type,
    const std::string& definition) {
  std::unordered_map<std::string, StoredValue> row;
  row["Name"] = make_string_value(name);
  row["ItemTypeUUID"] = make_string_value(item_type_uuid);
  row["Type"] = make_string_value(type);
  row["Definition"] = make_string_value(definition);
  return row;
}

static std::string build_domain_definition_xml(const std::string& domain_name, const DomainState& domain) {
  std::ostringstream out;
  out << "<CodedValueDomain name=\"" << domain_name << "\" fieldType=\"" << domain.field_type << "\">";
  for (const auto& coded_value : domain.coded_values) {
    out << "<CodedValue code=\"" << coded_value.first << "\" name=\"" << coded_value.second << "\"/>";
  }
  out << "</CodedValueDomain>";
  return out.str();
}

static std::string build_relationship_definition_xml(const std::string& name, const RelationshipState& rel) {
  std::ostringstream out;
  out << "<RelationshipClass name=\"" << name << "\" origin=\"" << rel.origin_table << "\" destination=\"" << rel.destination_table
      << "\" originPK=\"" << rel.origin_pk << "\" originFK=\"" << rel.origin_fk << "\" cardinality=\"" << rel.cardinality
      << "\" composite=\"" << (rel.composite ? "true" : "false") << "\" attributed=\"" << (rel.attributed ? "true" : "false")
      << "\"/>";
  return out.str();
}

static void rebuild_catalog_tables(DbState& db) {
  constexpr const char* kCodedDomainItemTypeUuid = "{8C5E4548-F3D3-11D4-9F42-00C04F6BC6A5}";
  constexpr const char* kRelationshipClassItemTypeUuid = "{725BADAB-3452-491B-A795-55F32D67229C}";

  TableState items;
  items.columns = {"Name", "ItemTypeUUID", "Type", "Definition"};
  for (const auto& domain_entry : db.domains) {
    items.rows.push_back(make_item_row(
        domain_entry.first,
        kCodedDomainItemTypeUuid,
        "Coded Value Domain",
        build_domain_definition_xml(domain_entry.first, domain_entry.second)));
  }
  for (const auto& rel_entry : db.relationships) {
    items.rows.push_back(make_item_row(
        rel_entry.first,
        kRelationshipClassItemTypeUuid,
        "Relationship Class",
        build_relationship_definition_xml(rel_entry.first, rel_entry.second)));
  }
  db.tables["GDB_Items"] = std::move(items);

  TableState item_relationships;
  item_relationships.columns = {"OriginName", "DestinationName", "RelationshipType"};
  for (const std::string& assignment : db.assignments) {
    std::vector<std::string> parts = split(assignment, '|');
    if (parts.size() != 3) {
      continue;
    }
    std::unordered_map<std::string, StoredValue> row;
    row["OriginName"] = make_string_value(parts[0]);
    row["DestinationName"] = make_string_value(parts[1] + "." + parts[2]);
    row["RelationshipType"] = make_string_value("DomainInDataset");
    item_relationships.rows.push_back(std::move(row));
  }
  for (const auto& rel_entry : db.relationships) {
    const std::string& rel_name = rel_entry.first;
    const RelationshipState& rel = rel_entry.second;
    {
      std::unordered_map<std::string, StoredValue> row;
      row["OriginName"] = make_string_value(rel.origin_table);
      row["DestinationName"] = make_string_value(rel_name);
      row["RelationshipType"] = make_string_value("OriginClassInRelationshipClass");
      item_relationships.rows.push_back(std::move(row));
    }
    {
      std::unordered_map<std::string, StoredValue> row;
      row["OriginName"] = make_string_value(rel.destination_table);
      row["DestinationName"] = make_string_value(rel_name);
      row["RelationshipType"] = make_string_value("DestinationClassInRelationshipClass");
      item_relationships.rows.push_back(std::move(row));
    }
    {
      std::unordered_map<std::string, StoredValue> row;
      row["OriginName"] = make_string_value(rel_name);
      row["DestinationName"] = make_string_value(rel.origin_pk);
      row["RelationshipType"] = make_string_value("ClassKey");
      item_relationships.rows.push_back(std::move(row));
    }
    {
      std::unordered_map<std::string, StoredValue> row;
      row["OriginName"] = make_string_value(rel_name);
      row["DestinationName"] = make_string_value(rel.origin_fk);
      row["RelationshipType"] = make_string_value("DatasetsRelatedThrough");
      item_relationships.rows.push_back(std::move(row));
    }
  }
  db.tables["GDB_ItemRelationships"] = std::move(item_relationships);
}

static bool ensure_dir(const std::string& path) {
  std::error_code ec;
  if (std::filesystem::exists(path, ec)) {
    return std::filesystem::is_directory(path, ec);
  }
  return std::filesystem::create_directories(path, ec);
}

int NativeAdapter::open(const char* path, uint64_t* db_handle) {
  if (path == nullptr || *path == '\0' || db_handle == nullptr) {
    return fail(OFGDB_ERR_INVALID_ARG, "path/output handle missing");
  }

  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);

  std::string root = normalize_db_path(path);
  if (!std::filesystem::exists(root)) {
    return fail(OFGDB_ERR_NOT_FOUND, "database path does not exist");
  }
  std::shared_ptr<DbState> db;
  auto cached = st.path_dbs.find(root);
  if (cached != st.path_dbs.end()) {
    db = cached->second;
  } else {
    db = std::make_shared<DbState>();
    db->root_path = root;
    rebuild_catalog_tables(*db);
    st.path_dbs[root] = db;
  }
  rebuild_catalog_tables(*db);
  uint64_t handle = st.allocate_handle();
  st.dbs[handle] = db;
  *db_handle = handle;
  return OFGDB_OK;
}

int NativeAdapter::create(const char* path, uint64_t* db_handle) {
  if (path == nullptr || *path == '\0' || db_handle == nullptr) {
    return fail(OFGDB_ERR_INVALID_ARG, "path/output handle missing");
  }

  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);

  std::string root = normalize_db_path(path);
  if (!ensure_dir(root)) {
    return fail(OFGDB_ERR_INTERNAL, "failed to create database directory");
  }
  auto db = std::make_shared<DbState>();
  db->root_path = root;
  rebuild_catalog_tables(*db);
  st.path_dbs[root] = db;
  uint64_t handle = st.allocate_handle();
  st.dbs[handle] = db;
  *db_handle = handle;
  return OFGDB_OK;
}

int NativeAdapter::close(uint64_t db_handle) {
  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);

  auto db_it = st.dbs.find(db_handle);
  if (db_it == st.dbs.end()) {
    return OFGDB_OK;
  }

  for (auto it = st.tables.begin(); it != st.tables.end();) {
    if (it->second.db_handle == db_handle) {
      it = st.tables.erase(it);
    } else {
      ++it;
    }
  }
  for (auto it = st.rows.begin(); it != st.rows.end();) {
    if (it->second.db_handle == db_handle) {
      it = st.rows.erase(it);
    } else {
      ++it;
    }
  }
  for (auto it = st.cursors.begin(); it != st.cursors.end();) {
    if (it->second.db_handle == db_handle) {
      it = st.cursors.erase(it);
    } else {
      ++it;
    }
  }
  for (auto it = st.field_infos.begin(); it != st.field_infos.end();) {
    if (it->second.db_handle == db_handle) {
      it = st.field_infos.erase(it);
    } else {
      ++it;
    }
  }
  st.dbs.erase(db_it);
  return OFGDB_OK;
}

static std::shared_ptr<DbState> get_db_locked(NativeState& st, uint64_t db_handle) {
  auto it = st.dbs.find(db_handle);
  if (it == st.dbs.end()) {
    return nullptr;
  }
  return it->second;
}

static std::unordered_map<std::string, TableState>::iterator find_table_ci(
    std::unordered_map<std::string, TableState>& tables,
    const std::string& table_name) {
  auto exact = tables.find(table_name);
  if (exact != tables.end()) {
    return exact;
  }
  std::string wanted = upper(table_name);
  for (auto it = tables.begin(); it != tables.end(); ++it) {
    if (upper(it->first) == wanted) {
      return it;
    }
  }
  return tables.end();
}

static std::unordered_map<std::string, TableState>::const_iterator find_table_ci(
    const std::unordered_map<std::string, TableState>& tables,
    const std::string& table_name) {
  auto exact = tables.find(table_name);
  if (exact != tables.end()) {
    return exact;
  }
  std::string wanted = upper(table_name);
  for (auto it = tables.begin(); it != tables.end(); ++it) {
    if (upper(it->first) == wanted) {
      return it;
    }
  }
  return tables.end();
}

static std::unordered_map<std::string, StoredValue>::const_iterator find_value_ci(
    const std::unordered_map<std::string, StoredValue>& row,
    const std::string& column_name) {
  auto exact = row.find(column_name);
  if (exact != row.end()) {
    return exact;
  }
  std::string wanted = upper(column_name);
  for (auto it = row.begin(); it != row.end(); ++it) {
    if (upper(it->first) == wanted) {
      return it;
    }
  }
  return row.end();
}

static bool row_matches_where(const std::unordered_map<std::string, StoredValue>& row, const std::string& where_clause);

int NativeAdapter::exec_sql(uint64_t db_handle, const char* sql) {
  if (sql == nullptr) {
    return fail(OFGDB_ERR_INVALID_ARG, "sql is null");
  }

  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);
  auto db = get_db_locked(st, db_handle);
  if (!db) {
    return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
  }

  std::string stmt = trim(sql);
  std::string stmt_upper = upper(stmt);

  if (stmt_upper.rfind("CREATE TABLE", 0) == 0) {
    size_t name_start = std::strlen("CREATE TABLE");
    size_t paren = stmt.find('(', name_start);
    if (paren == std::string::npos) {
      return fail(OFGDB_ERR_INVALID_ARG, "CREATE TABLE syntax unsupported");
    }
    std::string table_name = trim(stmt.substr(name_start, paren - name_start));
    size_t close = stmt.rfind(')');
    if (close == std::string::npos || close <= paren) {
      return fail(OFGDB_ERR_INVALID_ARG, "CREATE TABLE syntax unsupported");
    }
    std::string defs = stmt.substr(paren + 1, close - paren - 1);
    TableState table;
    for (const std::string& def : split(defs, ',')) {
      std::string trimmed = trim(def);
      if (trimmed.empty()) {
        continue;
      }
      std::vector<std::string> parts = split(trimmed, ' ');
      if (!parts.empty()) {
        table.columns.push_back(parts[0]);
      }
    }
    db->tables[table_name] = table;
    return OFGDB_OK;
  }

  if (stmt_upper.rfind("DELETE FROM", 0) == 0) {
    std::string table_name = trim(stmt.substr(std::strlen("DELETE FROM")));
    auto table_it = find_table_ci(db->tables, table_name);
    if (table_it == db->tables.end()) {
      return fail(OFGDB_ERR_NOT_FOUND, "table does not exist");
    }
    table_it->second.rows.clear();
    return OFGDB_OK;
  }

  if (stmt_upper.rfind("INSERT INTO", 0) == 0) {
    size_t into_start = std::strlen("INSERT INTO");
    size_t col_open = stmt.find('(', into_start);
    if (col_open == std::string::npos) {
      return fail(OFGDB_ERR_INVALID_ARG, "INSERT syntax unsupported");
    }
    std::string table_name = trim(stmt.substr(into_start, col_open - into_start));
    size_t col_close = stmt.find(')', col_open + 1);
    if (col_close == std::string::npos) {
      return fail(OFGDB_ERR_INVALID_ARG, "INSERT syntax unsupported");
    }
    size_t values_pos = upper(stmt).find("VALUES", col_close);
    if (values_pos == std::string::npos) {
      return fail(OFGDB_ERR_INVALID_ARG, "INSERT syntax unsupported");
    }
    size_t val_open = stmt.find('(', values_pos);
    size_t val_close = stmt.rfind(')');
    if (val_open == std::string::npos || val_close == std::string::npos || val_close <= val_open) {
      return fail(OFGDB_ERR_INVALID_ARG, "INSERT syntax unsupported");
    }

    auto table_it = find_table_ci(db->tables, table_name);
    if (table_it == db->tables.end()) {
      return fail(OFGDB_ERR_NOT_FOUND, "table does not exist");
    }

    std::vector<std::string> cols = split(stmt.substr(col_open + 1, col_close - col_open - 1), ',');
    std::vector<std::string> values = split_sql_values(stmt.substr(val_open + 1, val_close - val_open - 1));
    if (cols.size() != values.size()) {
      return fail(OFGDB_ERR_INVALID_ARG, "column/value count mismatch");
    }

    std::unordered_map<std::string, StoredValue> row;
    for (size_t i = 0; i < cols.size(); i++) {
      StoredValue v = parse_sql_literal(values[i]);
      row[cols[i]] = v;
    }
    table_it->second.rows.push_back(std::move(row));
    return OFGDB_OK;
  }

  if (stmt_upper.rfind("UPDATE", 0) == 0) {
    size_t set_pos = stmt_upper.find(" SET ");
    if (set_pos == std::string::npos) {
      return fail(OFGDB_ERR_INVALID_ARG, "UPDATE syntax unsupported");
    }
    std::string table_name = trim(stmt.substr(std::strlen("UPDATE"), set_pos - std::strlen("UPDATE")));
    auto table_it = find_table_ci(db->tables, table_name);
    if (table_it == db->tables.end()) {
      return fail(OFGDB_ERR_NOT_FOUND, "table does not exist");
    }

    size_t where_pos = stmt_upper.find(" WHERE ", set_pos + 5);
    std::string set_clause;
    std::string where_clause;
    if (where_pos == std::string::npos) {
      set_clause = stmt.substr(set_pos + 5);
      where_clause.clear();
    } else {
      set_clause = stmt.substr(set_pos + 5, where_pos - (set_pos + 5));
      where_clause = stmt.substr(where_pos + 7);
    }

    std::vector<std::pair<std::string, StoredValue>> assignments;
    std::vector<std::string> set_parts = split_sql_values(set_clause);
    for (const std::string& part : set_parts) {
      size_t eq = part.find('=');
      if (eq == std::string::npos) {
        return fail(OFGDB_ERR_INVALID_ARG, "UPDATE SET syntax unsupported");
      }
      std::string col = trim(part.substr(0, eq));
      StoredValue value = parse_sql_literal(part.substr(eq + 1));
      assignments.emplace_back(col, std::move(value));
    }

    for (auto& row : table_it->second.rows) {
      if (!row_matches_where(row, where_clause)) {
        continue;
      }
      for (const auto& assignment : assignments) {
        row[assignment.first] = assignment.second;
      }
    }
    return OFGDB_OK;
  }

  return OFGDB_OK;
}

int NativeAdapter::open_table(uint64_t db_handle, const char* table_name, uint64_t* table_handle) {
  if (table_name == nullptr || *table_name == '\0' || table_handle == nullptr) {
    return fail(OFGDB_ERR_INVALID_ARG, "table name/output handle missing");
  }

  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);
  auto db = get_db_locked(st, db_handle);
  if (!db) {
    return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
  }
  auto table_it = find_table_ci(db->tables, table_name);
  if (table_it == db->tables.end()) {
    return fail(OFGDB_ERR_NOT_FOUND, "table does not exist");
  }

  uint64_t handle = st.allocate_handle();
  st.tables[handle] = TableHandle{db_handle, table_it->first};
  *table_handle = handle;
  return OFGDB_OK;
}

int NativeAdapter::close_table(uint64_t db_handle, uint64_t table_handle) {
  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);

  auto it = st.tables.find(table_handle);
  if (it == st.tables.end()) {
    return OFGDB_OK;
  }
  if (it->second.db_handle != db_handle) {
    return fail(OFGDB_ERR_INVALID_ARG, "table handle does not belong to db");
  }
  st.tables.erase(it);
  return OFGDB_OK;
}

static bool row_matches_where(const std::unordered_map<std::string, StoredValue>& row, const std::string& where_clause) {
  std::string wc = trim(where_clause);
  if (wc.empty()) {
    return true;
  }
  std::vector<std::string> parts;
  std::string cur;
  std::string up = upper(wc);
  size_t i = 0;
  while (i < wc.size()) {
    if (i + 5 <= wc.size() && up.substr(i, 5) == " AND ") {
      parts.push_back(trim(cur));
      cur.clear();
      i += 5;
      continue;
    }
    cur.push_back(wc[i]);
    i++;
  }
  if (!cur.empty()) {
    parts.push_back(trim(cur));
  }

  for (const std::string& cond : parts) {
    std::string cond_up = upper(cond);
    size_t eq = cond.find('=');
    size_t is_null = cond_up.find(" IS NULL");
    if (is_null != std::string::npos) {
      std::string col = trim(cond.substr(0, is_null));
      auto it = find_value_ci(row, col);
      if (it == row.end() || it->second.type != StoredValue::Type::kNull) {
        return false;
      }
      continue;
    }
    if (eq == std::string::npos) {
      continue;
    }
    std::string col = trim(cond.substr(0, eq));
    std::string rhs = unquote(cond.substr(eq + 1));
    auto it = find_value_ci(row, col);
    if (it == row.end()) {
      return false;
    }
    if (it->second.type == StoredValue::Type::kString) {
      if (it->second.text != rhs) {
        return false;
      }
    } else if (it->second.type == StoredValue::Type::kInt) {
      if (std::to_string(it->second.int_value) != rhs) {
        return false;
      }
    } else {
      return false;
    }
  }
  return true;
}

int NativeAdapter::search(uint64_t table_handle, const char* fields, const char* where_clause, uint64_t* cursor_handle) {
  if (cursor_handle == nullptr) {
    return fail(OFGDB_ERR_INVALID_ARG, "output cursor handle missing");
  }

  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);

  auto table_it = st.tables.find(table_handle);
  if (table_it == st.tables.end()) {
    return fail(OFGDB_ERR_INVALID_ARG, "unknown table handle");
  }
  auto db = get_db_locked(st, table_it->second.db_handle);
  if (!db) {
    return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
  }
  auto db_table_it = db->tables.find(table_it->second.table_name);
  if (db_table_it == db->tables.end()) {
    return fail(OFGDB_ERR_NOT_FOUND, "table does not exist");
  }

  (void)fields;
  const char* wc = where_clause != nullptr ? where_clause : "";
  CursorHandle cursor;
  cursor.db_handle = table_it->second.db_handle;
  for (const auto& row : db_table_it->second.rows) {
    if (row_matches_where(row, wc)) {
      cursor.rows.push_back(row);
    }
  }

  uint64_t handle = st.allocate_handle();
  st.cursors[handle] = std::move(cursor);
  *cursor_handle = handle;
  return OFGDB_OK;
}

int NativeAdapter::fetch_row(uint64_t cursor_handle, uint64_t* row_handle) {
  if (row_handle == nullptr) {
    return fail(OFGDB_ERR_INVALID_ARG, "output row handle missing");
  }

  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);

  auto cursor_it = st.cursors.find(cursor_handle);
  if (cursor_it == st.cursors.end()) {
    return fail(OFGDB_ERR_INVALID_ARG, "unknown cursor handle");
  }

  CursorHandle& cursor = cursor_it->second;
  if (cursor.index >= cursor.rows.size()) {
    *row_handle = 0;
    return OFGDB_OK;
  }

  RowHandle row;
  row.db_handle = cursor.db_handle;
  row.values = cursor.rows[cursor.index++];

  uint64_t handle = st.allocate_handle();
  st.rows[handle] = std::move(row);
  *row_handle = handle;
  return OFGDB_OK;
}

int NativeAdapter::close_cursor(uint64_t cursor_handle) {
  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);
  auto it = st.cursors.find(cursor_handle);
  if (it == st.cursors.end()) {
    return OFGDB_OK;
  }
  st.cursors.erase(it);
  return OFGDB_OK;
}

int NativeAdapter::create_row(uint64_t table_handle, uint64_t* row_handle) {
  if (row_handle == nullptr) {
    return fail(OFGDB_ERR_INVALID_ARG, "output row handle missing");
  }

  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);
  auto table_it = st.tables.find(table_handle);
  if (table_it == st.tables.end()) {
    return fail(OFGDB_ERR_INVALID_ARG, "unknown table handle");
  }

  RowHandle row;
  row.db_handle = table_it->second.db_handle;
  row.table_name = table_it->second.table_name;

  uint64_t handle = st.allocate_handle();
  st.rows[handle] = std::move(row);
  *row_handle = handle;
  return OFGDB_OK;
}

int NativeAdapter::insert(uint64_t table_handle, uint64_t row_handle) {
  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);

  auto table_it = st.tables.find(table_handle);
  auto row_it = st.rows.find(row_handle);
  if (table_it == st.tables.end() || row_it == st.rows.end()) {
    return fail(OFGDB_ERR_INVALID_ARG, "unknown table or row handle");
  }
  auto db = get_db_locked(st, table_it->second.db_handle);
  if (!db) {
    return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
  }
  auto db_table_it = db->tables.find(table_it->second.table_name);
  if (db_table_it == db->tables.end()) {
    return fail(OFGDB_ERR_NOT_FOUND, "table does not exist");
  }
  db_table_it->second.rows.push_back(row_it->second.values);
  return OFGDB_OK;
}

static bool stored_values_equal(const StoredValue& lhs, const StoredValue& rhs) {
  if (lhs.type != rhs.type) {
    return false;
  }
  switch (lhs.type) {
    case StoredValue::Type::kNull:
      return true;
    case StoredValue::Type::kString:
      return lhs.text == rhs.text;
    case StoredValue::Type::kInt:
      return lhs.int_value == rhs.int_value;
    case StoredValue::Type::kDouble:
      return lhs.double_value == rhs.double_value;
    case StoredValue::Type::kBlob:
    case StoredValue::Type::kGeometry:
      return lhs.bytes == rhs.bytes;
  }
  return false;
}

int NativeAdapter::update(uint64_t table_handle, uint64_t row_handle) {
  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);

  auto table_it = st.tables.find(table_handle);
  auto row_it = st.rows.find(row_handle);
  if (table_it == st.tables.end() || row_it == st.rows.end()) {
    return fail(OFGDB_ERR_INVALID_ARG, "unknown table or row handle");
  }
  auto db = get_db_locked(st, table_it->second.db_handle);
  if (!db) {
    return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
  }
  auto db_table_it = db->tables.find(table_it->second.table_name);
  if (db_table_it == db->tables.end()) {
    return fail(OFGDB_ERR_NOT_FOUND, "table does not exist");
  }

  const char* key_candidates[] = {"T_Id", "T_ID", "OBJECTID"};
  const StoredValue* key_value = nullptr;
  std::string key_column;
  for (const char* candidate : key_candidates) {
    auto value_it = find_value_ci(row_it->second.values, candidate);
    if (value_it != row_it->second.values.end()) {
      key_column = candidate;
      key_value = &(value_it->second);
      break;
    }
  }
  if (key_value == nullptr || key_value->type == StoredValue::Type::kNull) {
    return fail(OFGDB_ERR_INVALID_ARG, "update requires key column (T_Id/T_ID/OBJECTID)");
  }

  for (auto& existing_row : db_table_it->second.rows) {
    auto existing_key_it = find_value_ci(existing_row, key_column);
    if (existing_key_it == existing_row.end()) {
      continue;
    }
    if (!stored_values_equal(existing_key_it->second, *key_value)) {
      continue;
    }
    for (const auto& value : row_it->second.values) {
      existing_row[value.first] = value.second;
    }
    return OFGDB_OK;
  }

  db_table_it->second.rows.push_back(row_it->second.values);
  return OFGDB_OK;
}

int NativeAdapter::close_row(uint64_t row_handle) {
  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);
  auto it = st.rows.find(row_handle);
  if (it == st.rows.end()) {
    return OFGDB_OK;
  }
  st.rows.erase(it);
  return OFGDB_OK;
}

int NativeAdapter::get_field_info(uint64_t table_handle, uint64_t* field_info_handle) {
  if (field_info_handle == nullptr) {
    return fail(OFGDB_ERR_INVALID_ARG, "output field info handle missing");
  }

  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);

  auto table_it = st.tables.find(table_handle);
  if (table_it == st.tables.end()) {
    return fail(OFGDB_ERR_INVALID_ARG, "unknown table handle");
  }
  auto db = get_db_locked(st, table_it->second.db_handle);
  if (!db) {
    return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
  }
  auto db_table_it = db->tables.find(table_it->second.table_name);
  if (db_table_it == db->tables.end()) {
    return fail(OFGDB_ERR_NOT_FOUND, "table does not exist");
  }

  FieldInfoHandle info;
  info.db_handle = table_it->second.db_handle;
  info.columns = db_table_it->second.columns;

  uint64_t handle = st.allocate_handle();
  st.field_infos[handle] = std::move(info);
  *field_info_handle = handle;
  return OFGDB_OK;
}

int NativeAdapter::close_field_info(uint64_t field_info_handle) {
  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);
  auto it = st.field_infos.find(field_info_handle);
  if (it == st.field_infos.end()) {
    return OFGDB_OK;
  }
  st.field_infos.erase(it);
  return OFGDB_OK;
}

int NativeAdapter::field_info_count(uint64_t field_info_handle, int32_t* out_count) {
  if (out_count == nullptr) {
    return fail(OFGDB_ERR_INVALID_ARG, "output count missing");
  }

  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);
  auto it = st.field_infos.find(field_info_handle);
  if (it == st.field_infos.end()) {
    return fail(OFGDB_ERR_INVALID_ARG, "unknown field info handle");
  }
  *out_count = static_cast<int32_t>(it->second.columns.size());
  return OFGDB_OK;
}

int NativeAdapter::field_info_name(uint64_t field_info_handle, int32_t index, char** out_name) {
  if (out_name == nullptr) {
    return fail(OFGDB_ERR_INVALID_ARG, "output name missing");
  }
  *out_name = nullptr;

  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);
  auto it = st.field_infos.find(field_info_handle);
  if (it == st.field_infos.end()) {
    return fail(OFGDB_ERR_INVALID_ARG, "unknown field info handle");
  }
  if (index < 0 || static_cast<size_t>(index) >= it->second.columns.size()) {
    return fail(OFGDB_ERR_INVALID_ARG, "field info index out of range");
  }
  char* duplicated = dup_cstr(it->second.columns[static_cast<size_t>(index)]);
  if (duplicated == nullptr) {
    return fail(OFGDB_ERR_INTERNAL, "out of memory");
  }
  *out_name = duplicated;
  return OFGDB_OK;
}

static int fail_row_value(const std::string& message) {
  NativeState::instance().last_error = message;
  return OFGDB_ERR_INVALID_ARG;
}

static int set_row_value(uint64_t row_handle, const char* column_name, StoredValue value) {
  if (column_name == nullptr || *column_name == '\0') {
    return fail_row_value("column name missing");
  }

  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);
  auto row_it = st.rows.find(row_handle);
  if (row_it == st.rows.end()) {
    return fail_row_value("unknown row handle");
  }
  row_it->second.values[column_name] = std::move(value);
  return OFGDB_OK;
}

int NativeAdapter::set_string(uint64_t row_handle, const char* column_name, const char* value) {
  StoredValue v;
  v.type = StoredValue::Type::kString;
  v.text = value != nullptr ? value : "";
  return set_row_value(row_handle, column_name, std::move(v));
}

int NativeAdapter::set_int32(uint64_t row_handle, const char* column_name, int32_t value) {
  StoredValue v;
  v.type = StoredValue::Type::kInt;
  v.int_value = value;
  return set_row_value(row_handle, column_name, std::move(v));
}

int NativeAdapter::set_double(uint64_t row_handle, const char* column_name, double value) {
  StoredValue v;
  v.type = StoredValue::Type::kDouble;
  v.double_value = value;
  return set_row_value(row_handle, column_name, std::move(v));
}

int NativeAdapter::set_blob(uint64_t row_handle, const char* column_name, const uint8_t* data, int32_t size) {
  if (size < 0) {
    return fail(OFGDB_ERR_INVALID_ARG, "blob size must be >= 0");
  }
  StoredValue v;
  v.type = StoredValue::Type::kBlob;
  if (data != nullptr && size > 0) {
    v.bytes.assign(data, data + size);
  }
  return set_row_value(row_handle, column_name, std::move(v));
}

int NativeAdapter::set_geometry(uint64_t row_handle, const uint8_t* wkb, int32_t size) {
  if (size < 0) {
    return fail(OFGDB_ERR_INVALID_ARG, "geometry size must be >= 0");
  }
  StoredValue v;
  v.type = StoredValue::Type::kGeometry;
  if (wkb != nullptr && size > 0) {
    v.bytes.assign(wkb, wkb + size);
  }
  return set_row_value(row_handle, "__geometry__", std::move(v));
}

int NativeAdapter::set_null(uint64_t row_handle, const char* column_name) {
  StoredValue v;
  v.type = StoredValue::Type::kNull;
  return set_row_value(row_handle, column_name, std::move(v));
}

int NativeAdapter::list_domains(uint64_t db_handle, uint64_t* cursor_handle) {
  if (cursor_handle == nullptr) {
    return fail(OFGDB_ERR_INVALID_ARG, "output cursor handle missing");
  }

  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);
  auto db = get_db_locked(st, db_handle);
  if (!db) {
    return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
  }

  CursorHandle cursor;
  cursor.db_handle = db_handle;
  for (const auto& domain_entry : db->domains) {
    std::unordered_map<std::string, StoredValue> row;
    StoredValue name;
    name.type = StoredValue::Type::kString;
    name.text = domain_entry.first;
    row["name"] = std::move(name);
    StoredValue type;
    type.type = StoredValue::Type::kString;
    type.text = domain_entry.second.field_type;
    row["fieldType"] = std::move(type);
    cursor.rows.push_back(std::move(row));
  }

  uint64_t handle = st.allocate_handle();
  st.cursors[handle] = std::move(cursor);
  *cursor_handle = handle;
  return OFGDB_OK;
}

int NativeAdapter::create_coded_domain(uint64_t db_handle, const char* domain_name, const char* field_type) {
  if (domain_name == nullptr || *domain_name == '\0') {
    return fail(OFGDB_ERR_INVALID_ARG, "domain name missing");
  }

  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);
  auto db = get_db_locked(st, db_handle);
  if (!db) {
    return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
  }
  auto existing_it = db->domains.find(domain_name);
  if (existing_it != db->domains.end()) {
    if (field_type != nullptr && *field_type != '\0' && upper(existing_it->second.field_type) != upper(field_type)) {
      existing_it->second.field_type = field_type;
      rebuild_catalog_tables(*db);
    }
    return OFGDB_OK;
  }
  DomainState domain;
  domain.field_type = field_type != nullptr ? field_type : "STRING";
  db->domains[domain_name] = std::move(domain);
  rebuild_catalog_tables(*db);
  return OFGDB_OK;
}

int NativeAdapter::add_coded_value(uint64_t db_handle, const char* domain_name, const char* code, const char* label) {
  if (domain_name == nullptr || code == nullptr) {
    return fail(OFGDB_ERR_INVALID_ARG, "domain/code missing");
  }

  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);
  auto db = get_db_locked(st, db_handle);
  if (!db) {
    return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
  }
  auto domain_it = db->domains.find(domain_name);
  if (domain_it == db->domains.end()) {
    return fail(OFGDB_ERR_NOT_FOUND, "domain does not exist");
  }
  domain_it->second.coded_values[code] = label != nullptr ? label : code;
  rebuild_catalog_tables(*db);
  return OFGDB_OK;
}

int NativeAdapter::assign_domain_to_field(uint64_t db_handle, const char* table_name, const char* column_name, const char* domain_name) {
  if (table_name == nullptr || column_name == nullptr || domain_name == nullptr) {
    return fail(OFGDB_ERR_INVALID_ARG, "table/column/domain missing");
  }

  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);
  auto db = get_db_locked(st, db_handle);
  if (!db) {
    return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
  }
  if (db->domains.find(domain_name) == db->domains.end()) {
    return fail(OFGDB_ERR_NOT_FOUND, "domain does not exist");
  }
  auto inserted = db->assignments.insert(std::string(domain_name) + "|" + table_name + "|" + column_name);
  if (inserted.second) {
    rebuild_catalog_tables(*db);
  }
  return OFGDB_OK;
}

int NativeAdapter::list_relationships(uint64_t db_handle, uint64_t* cursor_handle) {
  if (cursor_handle == nullptr) {
    return fail(OFGDB_ERR_INVALID_ARG, "output cursor handle missing");
  }

  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);
  auto db = get_db_locked(st, db_handle);
  if (!db) {
    return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
  }

  CursorHandle cursor;
  cursor.db_handle = db_handle;
  for (const auto& rel_entry : db->relationships) {
    std::unordered_map<std::string, StoredValue> row;
    StoredValue name;
    name.type = StoredValue::Type::kString;
    name.text = rel_entry.first;
    row["name"] = std::move(name);
    cursor.rows.push_back(std::move(row));
  }

  uint64_t handle = st.allocate_handle();
  st.cursors[handle] = std::move(cursor);
  *cursor_handle = handle;
  return OFGDB_OK;
}

int NativeAdapter::create_relationship_class(
    uint64_t db_handle,
    const char* name,
    const char* origin_table,
    const char* destination_table,
    const char* origin_pk,
    const char* origin_fk,
    const char* forward_label,
    const char* backward_label,
    const char* cardinality,
    int32_t is_composite,
    int32_t is_attributed) {
  if (name == nullptr || origin_table == nullptr || destination_table == nullptr || origin_pk == nullptr || origin_fk == nullptr ||
      cardinality == nullptr) {
    return fail(OFGDB_ERR_INVALID_ARG, "relationship input incomplete");
  }

  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);
  auto db = get_db_locked(st, db_handle);
  if (!db) {
    return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
  }
  RelationshipState rel;
  rel.origin_table = origin_table;
  rel.destination_table = destination_table;
  rel.origin_pk = origin_pk;
  rel.origin_fk = origin_fk;
  rel.forward_label = forward_label != nullptr ? forward_label : "";
  rel.backward_label = backward_label != nullptr ? backward_label : "";
  rel.cardinality = cardinality;
  rel.composite = is_composite != 0;
  rel.attributed = is_attributed != 0;

  auto has_same_signature = [](const RelationshipState& lhs, const RelationshipState& rhs) {
    return upper(lhs.origin_table) == upper(rhs.origin_table) &&
           upper(lhs.destination_table) == upper(rhs.destination_table) &&
           upper(lhs.origin_pk) == upper(rhs.origin_pk) &&
           upper(lhs.origin_fk) == upper(rhs.origin_fk) &&
           upper(lhs.cardinality) == upper(rhs.cardinality) &&
           lhs.composite == rhs.composite &&
           lhs.attributed == rhs.attributed;
  };

  auto existing_by_name = db->relationships.find(name);
  if (existing_by_name != db->relationships.end()) {
    if (has_same_signature(existing_by_name->second, rel)) {
      return OFGDB_OK;
    }
    return fail(OFGDB_ERR_ALREADY_EXISTS, "relationship already exists with different definition");
  }
  for (const auto& existing : db->relationships) {
    if (has_same_signature(existing.second, rel)) {
      return OFGDB_OK;
    }
  }

  db->relationships[name] = std::move(rel);
  rebuild_catalog_tables(*db);
  return OFGDB_OK;
}

static std::string join_lines(const std::vector<std::string>& values) {
  std::ostringstream out;
  for (size_t i = 0; i < values.size(); i++) {
    if (i > 0) {
      out << '\n';
    }
    out << values[i];
  }
  return out.str();
}

int NativeAdapter::list_domains_text(uint64_t db_handle, char** out_text) {
  if (out_text == nullptr) {
    return fail(OFGDB_ERR_INVALID_ARG, "output pointer missing");
  }
  *out_text = nullptr;

  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);
  auto db = get_db_locked(st, db_handle);
  if (!db) {
    return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
  }
  std::vector<std::string> values;
  values.reserve(db->domains.size());
  for (const auto& domain_entry : db->domains) {
    values.push_back(domain_entry.first);
  }
  std::string text = join_lines(values);
  char* duplicated = dup_cstr(text);
  if (duplicated == nullptr) {
    return fail(OFGDB_ERR_INTERNAL, "out of memory");
  }
  *out_text = duplicated;
  return OFGDB_OK;
}

int NativeAdapter::list_relationships_text(uint64_t db_handle, char** out_text) {
  if (out_text == nullptr) {
    return fail(OFGDB_ERR_INVALID_ARG, "output pointer missing");
  }
  *out_text = nullptr;

  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);
  auto db = get_db_locked(st, db_handle);
  if (!db) {
    return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
  }
  std::vector<std::string> values;
  values.reserve(db->relationships.size());
  for (const auto& rel_entry : db->relationships) {
    values.push_back(rel_entry.first);
  }
  std::string text = join_lines(values);
  char* duplicated = dup_cstr(text);
  if (duplicated == nullptr) {
    return fail(OFGDB_ERR_INTERNAL, "out of memory");
  }
  *out_text = duplicated;
  return OFGDB_OK;
}

int NativeAdapter::list_tables_text(uint64_t db_handle, char** out_text) {
  if (out_text == nullptr) {
    return fail(OFGDB_ERR_INVALID_ARG, "output pointer missing");
  }
  *out_text = nullptr;

  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);
  auto db = get_db_locked(st, db_handle);
  if (!db) {
    return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
  }
  std::vector<std::string> values;
  values.reserve(db->tables.size());
  for (const auto& table_entry : db->tables) {
    values.push_back(table_entry.first);
  }
  std::string text = join_lines(values);
  char* duplicated = dup_cstr(text);
  if (duplicated == nullptr) {
    return fail(OFGDB_ERR_INTERNAL, "out of memory");
  }
  *out_text = duplicated;
  return OFGDB_OK;
}

static int get_row_value_locked(NativeState& st, uint64_t row_handle, const char* column_name, const StoredValue** out_value) {
  auto row_it = st.rows.find(row_handle);
  if (row_it == st.rows.end()) {
    st.last_error = "unknown row handle";
    return OFGDB_ERR_INVALID_ARG;
  }
  auto value_it = find_value_ci(row_it->second.values, column_name);
  if (value_it == row_it->second.values.end()) {
    *out_value = nullptr;
    return OFGDB_OK;
  }
  *out_value = &(value_it->second);
  return OFGDB_OK;
}

int NativeAdapter::row_get_string(uint64_t row_handle, const char* column_name, char** out_value) {
  if (column_name == nullptr || out_value == nullptr) {
    return fail(OFGDB_ERR_INVALID_ARG, "column/output missing");
  }
  *out_value = nullptr;

  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);
  const StoredValue* value = nullptr;
  int rc = get_row_value_locked(st, row_handle, column_name, &value);
  if (rc != OFGDB_OK) {
    return rc;
  }
  if (value == nullptr || value->type == StoredValue::Type::kNull) {
    return OFGDB_OK;
  }

  std::string text;
  switch (value->type) {
    case StoredValue::Type::kString:
      text = value->text;
      break;
    case StoredValue::Type::kInt:
      text = std::to_string(value->int_value);
      break;
    case StoredValue::Type::kDouble:
      text = std::to_string(value->double_value);
      break;
    case StoredValue::Type::kBlob:
    case StoredValue::Type::kGeometry:
      text = "<binary>";
      break;
    case StoredValue::Type::kNull:
      return OFGDB_OK;
  }

  char* duplicated = dup_cstr(text);
  if (duplicated == nullptr) {
    return fail(OFGDB_ERR_INTERNAL, "out of memory");
  }
  *out_value = duplicated;
  return OFGDB_OK;
}

int NativeAdapter::row_is_null(uint64_t row_handle, const char* column_name, int32_t* out_is_null) {
  if (column_name == nullptr || out_is_null == nullptr) {
    return fail(OFGDB_ERR_INVALID_ARG, "column/output missing");
  }
  *out_is_null = 1;

  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);
  const StoredValue* value = nullptr;
  int rc = get_row_value_locked(st, row_handle, column_name, &value);
  if (rc != OFGDB_OK) {
    return rc;
  }
  if (value == nullptr || value->type == StoredValue::Type::kNull) {
    *out_is_null = 1;
    return OFGDB_OK;
  }
  *out_is_null = 0;
  return OFGDB_OK;
}

int NativeAdapter::row_get_int32(uint64_t row_handle, const char* column_name, int32_t* out_value) {
  if (column_name == nullptr || out_value == nullptr) {
    return fail(OFGDB_ERR_INVALID_ARG, "column/output missing");
  }
  *out_value = 0;

  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);
  const StoredValue* value = nullptr;
  int rc = get_row_value_locked(st, row_handle, column_name, &value);
  if (rc != OFGDB_OK) {
    return rc;
  }
  if (value == nullptr || value->type == StoredValue::Type::kNull) {
    return OFGDB_OK;
  }
  if (value->type == StoredValue::Type::kInt) {
    *out_value = value->int_value;
    return OFGDB_OK;
  }
  return fail(OFGDB_ERR_INVALID_ARG, "row value is not int32");
}

int NativeAdapter::row_get_double(uint64_t row_handle, const char* column_name, double* out_value) {
  if (column_name == nullptr || out_value == nullptr) {
    return fail(OFGDB_ERR_INVALID_ARG, "column/output missing");
  }
  *out_value = 0.0;

  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);
  const StoredValue* value = nullptr;
  int rc = get_row_value_locked(st, row_handle, column_name, &value);
  if (rc != OFGDB_OK) {
    return rc;
  }
  if (value == nullptr || value->type == StoredValue::Type::kNull) {
    return OFGDB_OK;
  }
  if (value->type == StoredValue::Type::kDouble) {
    *out_value = value->double_value;
    return OFGDB_OK;
  }
  if (value->type == StoredValue::Type::kInt) {
    *out_value = static_cast<double>(value->int_value);
    return OFGDB_OK;
  }
  return fail(OFGDB_ERR_INVALID_ARG, "row value is not double");
}

int NativeAdapter::row_get_blob(uint64_t row_handle, const char* column_name, uint8_t** out_data, int32_t* out_size) {
  if (column_name == nullptr || out_data == nullptr || out_size == nullptr) {
    return fail(OFGDB_ERR_INVALID_ARG, "column/output missing");
  }
  *out_data = nullptr;
  *out_size = 0;

  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);
  const StoredValue* value = nullptr;
  int rc = get_row_value_locked(st, row_handle, column_name, &value);
  if (rc != OFGDB_OK) {
    return rc;
  }
  if (value == nullptr || value->type == StoredValue::Type::kNull) {
    return OFGDB_OK;
  }
  if (value->type != StoredValue::Type::kBlob) {
    return fail(OFGDB_ERR_INVALID_ARG, "row value is not blob");
  }
  if (value->bytes.size() > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
    return fail(OFGDB_ERR_INTERNAL, "blob too large");
  }
  *out_size = static_cast<int32_t>(value->bytes.size());
  if (!value->bytes.empty()) {
    uint8_t* duplicated = dup_bytes(value->bytes);
    if (duplicated == nullptr) {
      return fail(OFGDB_ERR_INTERNAL, "out of memory");
    }
    *out_data = duplicated;
  }
  return OFGDB_OK;
}

int NativeAdapter::row_get_geometry(uint64_t row_handle, uint8_t** out_wkb, int32_t* out_size) {
  if (out_wkb == nullptr || out_size == nullptr) {
    return fail(OFGDB_ERR_INVALID_ARG, "output missing");
  }
  *out_wkb = nullptr;
  *out_size = 0;

  NativeState& st = NativeState::instance();
  std::lock_guard<std::mutex> lock(st.mutex);
  auto row_it = st.rows.find(row_handle);
  if (row_it == st.rows.end()) {
    return fail(OFGDB_ERR_INVALID_ARG, "unknown row handle");
  }
  auto value_it = row_it->second.values.find("__geometry__");
  if (value_it == row_it->second.values.end() || value_it->second.type == StoredValue::Type::kNull) {
    return OFGDB_OK;
  }
  if (value_it->second.type != StoredValue::Type::kGeometry) {
    return fail(OFGDB_ERR_INVALID_ARG, "row geometry value has invalid type");
  }
  if (value_it->second.bytes.size() > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
    return fail(OFGDB_ERR_INTERNAL, "geometry too large");
  }
  *out_size = static_cast<int32_t>(value_it->second.bytes.size());
  if (!value_it->second.bytes.empty()) {
    uint8_t* duplicated = dup_bytes(value_it->second.bytes);
    if (duplicated == nullptr) {
      return fail(OFGDB_ERR_INTERNAL, "out of memory");
    }
    *out_wkb = duplicated;
  }
  return OFGDB_OK;
}

const char* NativeAdapter::last_error_message() const {
  return NativeState::instance().last_error.c_str();
}

}  // namespace openfgdb
