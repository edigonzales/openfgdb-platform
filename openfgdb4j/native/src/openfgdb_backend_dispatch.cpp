#include "openfgdb_backend.hpp"

#include "openfgdb_c_api.h"

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

namespace {

std::string to_lower_copy(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
    return static_cast<char>(std::tolower(c));
  });
  return value;
}

bool debug_enabled() {
  const char* value = std::getenv("OPENFGDB4J_DEBUG");
  if (value == nullptr) {
    return false;
  }
  std::string normalized = to_lower_copy(value);
  return normalized == "1" || normalized == "true" || normalized == "yes";
}

void log_debug(const std::string& msg) {
  if (!debug_enabled()) {
    return;
  }
  std::fprintf(stderr, "openfgdb4j: %s\n", msg.c_str());
}

char* dup_cstr(const std::string& value) {
  char* out = static_cast<char*>(std::malloc(value.size() + 1));
  if (out == nullptr) {
    return nullptr;
  }
  std::memcpy(out, value.c_str(), value.size() + 1);
  return out;
}

}  // namespace

namespace openfgdb {

BackendDispatch::BackendDispatch() = default;

BackendDispatch& BackendDispatch::instance() {
  static BackendDispatch dispatch;
  return dispatch;
}

int BackendDispatch::fail_local(int code, const std::string& message) {
  last_error_ = message;
  return code;
}

OpenFgdbBackend* BackendDispatch::backend() {
  return backend_.get();
}

const OpenFgdbBackend* BackendDispatch::backend() const {
  return backend_.get();
}

int BackendDispatch::ensure_backend_selected(const char* operation) {
  if (backend_) {
    return OFGDB_OK;
  }

  const char* backend_env = std::getenv("OPENFGDB4J_BACKEND");
  std::string backend_mode = backend_env != nullptr ? backend_env : "";
  if (backend_mode.empty()) {
    backend_mode = "gdal";
  }
  backend_mode = to_lower_copy(backend_mode);

  if (backend_mode == "adapter") {
    backend_ = create_adapter_backend();
  } else if (backend_mode == "gdal") {
    backend_ = create_gdal_backend();
  } else {
    return fail_local(
        OFGDB_ERR_INVALID_ARG,
        std::string("invalid OPENFGDB4J_BACKEND='") + backend_mode + "'; expected 'gdal' or 'adapter'");
  }

  log_debug(std::string("selected backend '") + backend_->backend_name() + "' for operation " + operation);
  return OFGDB_OK;
}

int BackendDispatch::invoke_and_capture(int code_from_backend) {
  if (code_from_backend == OFGDB_OK) {
    return OFGDB_OK;
  }
  if (backend_ != nullptr) {
    const char* backend_err = backend_->last_error_message();
    if (backend_err != nullptr && *backend_err != '\0') {
      last_error_ = std::string(backend_->backend_name()) + " backend failed: " + backend_err;
      return code_from_backend;
    }
  }
  if (last_error_.empty()) {
    last_error_ = "backend operation failed";
  }
  return code_from_backend;
}

int BackendDispatch::open(const char* path, uint64_t* db_handle) {
  int rc = ensure_backend_selected("open");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->open(path, db_handle));
}

int BackendDispatch::create(const char* path, uint64_t* db_handle) {
  int rc = ensure_backend_selected("create");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->create(path, db_handle));
}

int BackendDispatch::close(uint64_t db_handle) {
  int rc = ensure_backend_selected("close");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->close(db_handle));
}

int BackendDispatch::exec_sql(uint64_t db_handle, const char* sql) {
  int rc = ensure_backend_selected("exec_sql");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->exec_sql(db_handle, sql));
}

int BackendDispatch::open_table(uint64_t db_handle, const char* table_name, uint64_t* table_handle) {
  int rc = ensure_backend_selected("open_table");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->open_table(db_handle, table_name, table_handle));
}

int BackendDispatch::close_table(uint64_t db_handle, uint64_t table_handle) {
  int rc = ensure_backend_selected("close_table");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->close_table(db_handle, table_handle));
}

int BackendDispatch::search(uint64_t table_handle, const char* fields, const char* where_clause, uint64_t* cursor_handle) {
  int rc = ensure_backend_selected("search");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->search(table_handle, fields, where_clause, cursor_handle));
}

int BackendDispatch::fetch_row(uint64_t cursor_handle, uint64_t* row_handle) {
  int rc = ensure_backend_selected("fetch_row");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->fetch_row(cursor_handle, row_handle));
}

int BackendDispatch::close_cursor(uint64_t cursor_handle) {
  int rc = ensure_backend_selected("close_cursor");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->close_cursor(cursor_handle));
}

int BackendDispatch::create_row(uint64_t table_handle, uint64_t* row_handle) {
  int rc = ensure_backend_selected("create_row");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->create_row(table_handle, row_handle));
}

int BackendDispatch::insert(uint64_t table_handle, uint64_t row_handle) {
  int rc = ensure_backend_selected("insert");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->insert(table_handle, row_handle));
}

int BackendDispatch::update(uint64_t table_handle, uint64_t row_handle) {
  int rc = ensure_backend_selected("update");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->update(table_handle, row_handle));
}

int BackendDispatch::close_row(uint64_t row_handle) {
  int rc = ensure_backend_selected("close_row");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->close_row(row_handle));
}

int BackendDispatch::get_field_info(uint64_t table_handle, uint64_t* field_info_handle) {
  int rc = ensure_backend_selected("get_field_info");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->get_field_info(table_handle, field_info_handle));
}

int BackendDispatch::close_field_info(uint64_t field_info_handle) {
  int rc = ensure_backend_selected("close_field_info");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->close_field_info(field_info_handle));
}

int BackendDispatch::field_info_count(uint64_t field_info_handle, int32_t* out_count) {
  int rc = ensure_backend_selected("field_info_count");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->field_info_count(field_info_handle, out_count));
}

int BackendDispatch::field_info_name(uint64_t field_info_handle, int32_t index, char** out_name) {
  int rc = ensure_backend_selected("field_info_name");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->field_info_name(field_info_handle, index, out_name));
}

int BackendDispatch::set_string(uint64_t row_handle, const char* column_name, const char* value) {
  int rc = ensure_backend_selected("set_string");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->set_string(row_handle, column_name, value));
}

int BackendDispatch::set_int32(uint64_t row_handle, const char* column_name, int32_t value) {
  int rc = ensure_backend_selected("set_int32");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->set_int32(row_handle, column_name, value));
}

int BackendDispatch::set_double(uint64_t row_handle, const char* column_name, double value) {
  int rc = ensure_backend_selected("set_double");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->set_double(row_handle, column_name, value));
}

int BackendDispatch::set_blob(uint64_t row_handle, const char* column_name, const uint8_t* data, int32_t size) {
  int rc = ensure_backend_selected("set_blob");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->set_blob(row_handle, column_name, data, size));
}

int BackendDispatch::set_geometry(uint64_t row_handle, const uint8_t* wkb, int32_t size) {
  int rc = ensure_backend_selected("set_geometry");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->set_geometry(row_handle, wkb, size));
}

int BackendDispatch::set_null(uint64_t row_handle, const char* column_name) {
  int rc = ensure_backend_selected("set_null");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->set_null(row_handle, column_name));
}

int BackendDispatch::list_domains(uint64_t db_handle, uint64_t* cursor_handle) {
  int rc = ensure_backend_selected("list_domains");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->list_domains(db_handle, cursor_handle));
}

int BackendDispatch::create_coded_domain(uint64_t db_handle, const char* domain_name, const char* field_type) {
  int rc = ensure_backend_selected("create_coded_domain");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->create_coded_domain(db_handle, domain_name, field_type));
}

int BackendDispatch::add_coded_value(uint64_t db_handle, const char* domain_name, const char* code, const char* label) {
  int rc = ensure_backend_selected("add_coded_value");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->add_coded_value(db_handle, domain_name, code, label));
}

int BackendDispatch::assign_domain_to_field(uint64_t db_handle, const char* table_name, const char* column_name, const char* domain_name) {
  int rc = ensure_backend_selected("assign_domain_to_field");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->assign_domain_to_field(db_handle, table_name, column_name, domain_name));
}

int BackendDispatch::list_relationships(uint64_t db_handle, uint64_t* cursor_handle) {
  int rc = ensure_backend_selected("list_relationships");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->list_relationships(db_handle, cursor_handle));
}

int BackendDispatch::create_relationship_class(
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
  int rc = ensure_backend_selected("create_relationship_class");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->create_relationship_class(
      db_handle,
      name,
      origin_table,
      destination_table,
      origin_pk,
      origin_fk,
      forward_label,
      backward_label,
      cardinality,
      is_composite,
      is_attributed));
}

int BackendDispatch::list_domains_text(uint64_t db_handle, char** out_text) {
  int rc = ensure_backend_selected("list_domains_text");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->list_domains_text(db_handle, out_text));
}

int BackendDispatch::list_relationships_text(uint64_t db_handle, char** out_text) {
  int rc = ensure_backend_selected("list_relationships_text");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->list_relationships_text(db_handle, out_text));
}

int BackendDispatch::list_tables_text(uint64_t db_handle, char** out_text) {
  int rc = ensure_backend_selected("list_tables_text");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->list_tables_text(db_handle, out_text));
}

int BackendDispatch::row_get_string(uint64_t row_handle, const char* column_name, char** out_value) {
  int rc = ensure_backend_selected("row_get_string");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->row_get_string(row_handle, column_name, out_value));
}

int BackendDispatch::row_is_null(uint64_t row_handle, const char* column_name, int32_t* out_is_null) {
  int rc = ensure_backend_selected("row_is_null");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->row_is_null(row_handle, column_name, out_is_null));
}

int BackendDispatch::row_get_int32(uint64_t row_handle, const char* column_name, int32_t* out_value) {
  int rc = ensure_backend_selected("row_get_int32");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->row_get_int32(row_handle, column_name, out_value));
}

int BackendDispatch::row_get_double(uint64_t row_handle, const char* column_name, double* out_value) {
  int rc = ensure_backend_selected("row_get_double");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->row_get_double(row_handle, column_name, out_value));
}

int BackendDispatch::row_get_blob(uint64_t row_handle, const char* column_name, uint8_t** out_data, int32_t* out_size) {
  int rc = ensure_backend_selected("row_get_blob");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->row_get_blob(row_handle, column_name, out_data, out_size));
}

int BackendDispatch::row_get_geometry(uint64_t row_handle, uint8_t** out_wkb, int32_t* out_size) {
  int rc = ensure_backend_selected("row_get_geometry");
  if (rc != OFGDB_OK) {
    return rc;
  }
  return invoke_and_capture(backend()->row_get_geometry(row_handle, out_wkb, out_size));
}

int BackendDispatch::list_runtime_info_text(char** out_text) {
  if (out_text == nullptr) {
    return fail_local(OFGDB_ERR_INVALID_ARG, "output pointer missing");
  }
  *out_text = nullptr;

  int rc = ensure_backend_selected("list_runtime_info_text");
  if (rc != OFGDB_OK) {
    return rc;
  }
  std::string text = backend()->runtime_info();
  char* duplicated = dup_cstr(text);
  if (duplicated == nullptr) {
    return fail_local(OFGDB_ERR_INTERNAL, "out of memory");
  }
  *out_text = duplicated;
  return OFGDB_OK;
}

const char* BackendDispatch::last_error_message() const {
  if (!last_error_.empty()) {
    return last_error_.c_str();
  }
  if (backend_ != nullptr) {
    const char* backend_error = backend_->last_error_message();
    if (backend_error != nullptr && *backend_error != '\0') {
      return backend_error;
    }
  }
  return "";
}

}  // namespace openfgdb
