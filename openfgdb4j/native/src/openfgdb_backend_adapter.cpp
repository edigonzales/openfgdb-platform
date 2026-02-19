#include "openfgdb_backend.hpp"

#include "openfgdb_c_api.h"
#include "openfgdb_native_adapter.hpp"

#include <memory>
#include <string>

namespace openfgdb {

class AdapterBackend final : public OpenFgdbBackend {
 public:
  const char* backend_name() const override {
    return "adapter";
  }
  std::string runtime_info() const override {
    return "backend=adapter;impl=native_adapter;gdal_tag=v3.12.0";
  }
  const char* last_error_message() const override {
    return NativeAdapter::instance().last_error_message();
  }

  int open(const char* path, uint64_t* db_handle) override { return NativeAdapter::instance().open(path, db_handle); }
  int create(const char* path, uint64_t* db_handle) override { return NativeAdapter::instance().create(path, db_handle); }
  int close(uint64_t db_handle) override { return NativeAdapter::instance().close(db_handle); }
  int exec_sql(uint64_t db_handle, const char* sql) override { return NativeAdapter::instance().exec_sql(db_handle, sql); }
  int open_table(uint64_t db_handle, const char* table_name, uint64_t* table_handle) override {
    return NativeAdapter::instance().open_table(db_handle, table_name, table_handle);
  }
  int close_table(uint64_t db_handle, uint64_t table_handle) override {
    return NativeAdapter::instance().close_table(db_handle, table_handle);
  }
  int search(uint64_t table_handle, const char* fields, const char* where_clause, uint64_t* cursor_handle) override {
    return NativeAdapter::instance().search(table_handle, fields, where_clause, cursor_handle);
  }
  int fetch_row(uint64_t cursor_handle, uint64_t* row_handle) override {
    return NativeAdapter::instance().fetch_row(cursor_handle, row_handle);
  }
  int close_cursor(uint64_t cursor_handle) override { return NativeAdapter::instance().close_cursor(cursor_handle); }
  int create_row(uint64_t table_handle, uint64_t* row_handle) override {
    return NativeAdapter::instance().create_row(table_handle, row_handle);
  }
  int insert(uint64_t table_handle, uint64_t row_handle) override { return NativeAdapter::instance().insert(table_handle, row_handle); }
  int update(uint64_t table_handle, uint64_t row_handle) override { return NativeAdapter::instance().update(table_handle, row_handle); }
  int close_row(uint64_t row_handle) override { return NativeAdapter::instance().close_row(row_handle); }
  int get_field_info(uint64_t table_handle, uint64_t* field_info_handle) override {
    return NativeAdapter::instance().get_field_info(table_handle, field_info_handle);
  }
  int close_field_info(uint64_t field_info_handle) override { return NativeAdapter::instance().close_field_info(field_info_handle); }
  int field_info_count(uint64_t field_info_handle, int32_t* out_count) override {
    return NativeAdapter::instance().field_info_count(field_info_handle, out_count);
  }
  int field_info_name(uint64_t field_info_handle, int32_t index, char** out_name) override {
    return NativeAdapter::instance().field_info_name(field_info_handle, index, out_name);
  }
  int set_string(uint64_t row_handle, const char* column_name, const char* value) override {
    return NativeAdapter::instance().set_string(row_handle, column_name, value);
  }
  int set_int32(uint64_t row_handle, const char* column_name, int32_t value) override {
    return NativeAdapter::instance().set_int32(row_handle, column_name, value);
  }
  int set_double(uint64_t row_handle, const char* column_name, double value) override {
    return NativeAdapter::instance().set_double(row_handle, column_name, value);
  }
  int set_blob(uint64_t row_handle, const char* column_name, const uint8_t* data, int32_t size) override {
    return NativeAdapter::instance().set_blob(row_handle, column_name, data, size);
  }
  int set_geometry(uint64_t row_handle, const uint8_t* wkb, int32_t size) override {
    return NativeAdapter::instance().set_geometry(row_handle, wkb, size);
  }
  int set_null(uint64_t row_handle, const char* column_name) override {
    return NativeAdapter::instance().set_null(row_handle, column_name);
  }
  int list_domains(uint64_t db_handle, uint64_t* cursor_handle) override {
    return NativeAdapter::instance().list_domains(db_handle, cursor_handle);
  }
  int create_coded_domain(uint64_t db_handle, const char* domain_name, const char* field_type) override {
    return NativeAdapter::instance().create_coded_domain(db_handle, domain_name, field_type);
  }
  int add_coded_value(uint64_t db_handle, const char* domain_name, const char* code, const char* label) override {
    return NativeAdapter::instance().add_coded_value(db_handle, domain_name, code, label);
  }
  int assign_domain_to_field(uint64_t db_handle, const char* table_name, const char* column_name, const char* domain_name) override {
    return NativeAdapter::instance().assign_domain_to_field(db_handle, table_name, column_name, domain_name);
  }
  int list_relationships(uint64_t db_handle, uint64_t* cursor_handle) override {
    return NativeAdapter::instance().list_relationships(db_handle, cursor_handle);
  }
  int create_relationship_class(
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
      int32_t is_attributed) override {
    return NativeAdapter::instance().create_relationship_class(
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
  int list_domains_text(uint64_t db_handle, char** out_text) override {
    return NativeAdapter::instance().list_domains_text(db_handle, out_text);
  }
  int list_relationships_text(uint64_t db_handle, char** out_text) override {
    return NativeAdapter::instance().list_relationships_text(db_handle, out_text);
  }
  int list_tables_text(uint64_t db_handle, char** out_text) override {
    return NativeAdapter::instance().list_tables_text(db_handle, out_text);
  }
  int row_get_string(uint64_t row_handle, const char* column_name, char** out_value) override {
    return NativeAdapter::instance().row_get_string(row_handle, column_name, out_value);
  }
  int row_is_null(uint64_t row_handle, const char* column_name, int32_t* out_is_null) override {
    return NativeAdapter::instance().row_is_null(row_handle, column_name, out_is_null);
  }
  int row_get_int32(uint64_t row_handle, const char* column_name, int32_t* out_value) override {
    return NativeAdapter::instance().row_get_int32(row_handle, column_name, out_value);
  }
  int row_get_double(uint64_t row_handle, const char* column_name, double* out_value) override {
    return NativeAdapter::instance().row_get_double(row_handle, column_name, out_value);
  }
  int row_get_blob(uint64_t row_handle, const char* column_name, uint8_t** out_data, int32_t* out_size) override {
    return NativeAdapter::instance().row_get_blob(row_handle, column_name, out_data, out_size);
  }
  int row_get_geometry(uint64_t row_handle, uint8_t** out_wkb, int32_t* out_size) override {
    return NativeAdapter::instance().row_get_geometry(row_handle, out_wkb, out_size);
  }
};

std::unique_ptr<OpenFgdbBackend> create_adapter_backend() {
  return std::unique_ptr<OpenFgdbBackend>(new AdapterBackend());
}

}  // namespace openfgdb
