#include "openfgdb_c_api.h"

#include "openfgdb_backend.hpp"

#include <cstdlib>

using openfgdb::BackendDispatch;

int ofgdb_open(const char* path, uint64_t* db_handle) {
  return BackendDispatch::instance().open(path, db_handle);
}

int ofgdb_create(const char* path, uint64_t* db_handle) {
  return BackendDispatch::instance().create(path, db_handle);
}

int ofgdb_close(uint64_t db_handle) {
  return BackendDispatch::instance().close(db_handle);
}

int ofgdb_exec_sql(uint64_t db_handle, const char* sql) {
  return BackendDispatch::instance().exec_sql(db_handle, sql);
}

int ofgdb_open_table(uint64_t db_handle, const char* table_name, uint64_t* table_handle) {
  return BackendDispatch::instance().open_table(db_handle, table_name, table_handle);
}

int ofgdb_close_table(uint64_t db_handle, uint64_t table_handle) {
  return BackendDispatch::instance().close_table(db_handle, table_handle);
}

int ofgdb_search(uint64_t table_handle, const char* fields, const char* where_clause, uint64_t* cursor_handle) {
  return BackendDispatch::instance().search(table_handle, fields, where_clause, cursor_handle);
}

int ofgdb_fetch_row(uint64_t cursor_handle, uint64_t* row_handle) {
  return BackendDispatch::instance().fetch_row(cursor_handle, row_handle);
}

int ofgdb_close_cursor(uint64_t cursor_handle) {
  return BackendDispatch::instance().close_cursor(cursor_handle);
}

int ofgdb_create_row(uint64_t table_handle, uint64_t* row_handle) {
  return BackendDispatch::instance().create_row(table_handle, row_handle);
}

int ofgdb_insert(uint64_t table_handle, uint64_t row_handle) {
  return BackendDispatch::instance().insert(table_handle, row_handle);
}

int ofgdb_update(uint64_t table_handle, uint64_t row_handle) {
  return BackendDispatch::instance().update(table_handle, row_handle);
}

int ofgdb_close_row(uint64_t row_handle) {
  return BackendDispatch::instance().close_row(row_handle);
}

int ofgdb_get_field_info(uint64_t table_handle, uint64_t* field_info_handle) {
  return BackendDispatch::instance().get_field_info(table_handle, field_info_handle);
}

int ofgdb_close_field_info(uint64_t field_info_handle) {
  return BackendDispatch::instance().close_field_info(field_info_handle);
}

int ofgdb_field_info_count(uint64_t field_info_handle, int32_t* out_count) {
  return BackendDispatch::instance().field_info_count(field_info_handle, out_count);
}

int ofgdb_field_info_name(uint64_t field_info_handle, int32_t index, char** out_name) {
  return BackendDispatch::instance().field_info_name(field_info_handle, index, out_name);
}

int ofgdb_set_string(uint64_t row_handle, const char* column_name, const char* value) {
  return BackendDispatch::instance().set_string(row_handle, column_name, value);
}

int ofgdb_set_int32(uint64_t row_handle, const char* column_name, int32_t value) {
  return BackendDispatch::instance().set_int32(row_handle, column_name, value);
}

int ofgdb_set_double(uint64_t row_handle, const char* column_name, double value) {
  return BackendDispatch::instance().set_double(row_handle, column_name, value);
}

int ofgdb_set_blob(uint64_t row_handle, const char* column_name, const uint8_t* data, int32_t size) {
  return BackendDispatch::instance().set_blob(row_handle, column_name, data, size);
}

int ofgdb_set_geometry(uint64_t row_handle, const uint8_t* wkb, int32_t size) {
  return BackendDispatch::instance().set_geometry(row_handle, wkb, size);
}

int ofgdb_set_null(uint64_t row_handle, const char* column_name) {
  return BackendDispatch::instance().set_null(row_handle, column_name);
}

int ofgdb_list_domains(uint64_t db_handle, uint64_t* cursor_handle) {
  return BackendDispatch::instance().list_domains(db_handle, cursor_handle);
}

int ofgdb_create_coded_domain(uint64_t db_handle, const char* domain_name, const char* field_type) {
  return BackendDispatch::instance().create_coded_domain(db_handle, domain_name, field_type);
}

int ofgdb_add_coded_value(uint64_t db_handle, const char* domain_name, const char* code, const char* label) {
  return BackendDispatch::instance().add_coded_value(db_handle, domain_name, code, label);
}

int ofgdb_assign_domain_to_field(uint64_t db_handle, const char* table_name, const char* column_name, const char* domain_name) {
  return BackendDispatch::instance().assign_domain_to_field(db_handle, table_name, column_name, domain_name);
}

int ofgdb_list_relationships(uint64_t db_handle, uint64_t* cursor_handle) {
  return BackendDispatch::instance().list_relationships(db_handle, cursor_handle);
}

int ofgdb_create_relationship_class(
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
  return BackendDispatch::instance().create_relationship_class(
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
      is_attributed);
}

int ofgdb_list_domains_text(uint64_t db_handle, char** out_text) {
  return BackendDispatch::instance().list_domains_text(db_handle, out_text);
}

int ofgdb_list_relationships_text(uint64_t db_handle, char** out_text) {
  return BackendDispatch::instance().list_relationships_text(db_handle, out_text);
}

int ofgdb_list_tables_text(uint64_t db_handle, char** out_text) {
  return BackendDispatch::instance().list_tables_text(db_handle, out_text);
}

int ofgdb_list_runtime_info_text(char** out_text) {
  return BackendDispatch::instance().list_runtime_info_text(out_text);
}

int ofgdb_row_get_string(uint64_t row_handle, const char* column_name, char** out_value) {
  return BackendDispatch::instance().row_get_string(row_handle, column_name, out_value);
}

int ofgdb_row_is_null(uint64_t row_handle, const char* column_name, int32_t* out_is_null) {
  return BackendDispatch::instance().row_is_null(row_handle, column_name, out_is_null);
}

int ofgdb_row_get_int32(uint64_t row_handle, const char* column_name, int32_t* out_value) {
  return BackendDispatch::instance().row_get_int32(row_handle, column_name, out_value);
}

int ofgdb_row_get_double(uint64_t row_handle, const char* column_name, double* out_value) {
  return BackendDispatch::instance().row_get_double(row_handle, column_name, out_value);
}

int ofgdb_row_get_blob(uint64_t row_handle, const char* column_name, uint8_t** out_data, int32_t* out_size) {
  return BackendDispatch::instance().row_get_blob(row_handle, column_name, out_data, out_size);
}

int ofgdb_row_get_geometry(uint64_t row_handle, uint8_t** out_wkb, int32_t* out_size) {
  return BackendDispatch::instance().row_get_geometry(row_handle, out_wkb, out_size);
}

const char* ofgdb_last_error_message(void) {
  return BackendDispatch::instance().last_error_message();
}

void ofgdb_free_string(char* value) {
  std::free(value);
}
