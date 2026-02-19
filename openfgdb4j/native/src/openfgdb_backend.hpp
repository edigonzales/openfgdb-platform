#ifndef OPENFGDB_BACKEND_HPP
#define OPENFGDB_BACKEND_HPP

#include <cstdint>
#include <memory>
#include <string>

namespace openfgdb {

class OpenFgdbBackend {
 public:
  virtual ~OpenFgdbBackend() = default;

  virtual const char* backend_name() const = 0;
  virtual std::string runtime_info() const = 0;
  virtual const char* last_error_message() const = 0;

  virtual int open(const char* path, uint64_t* db_handle) = 0;
  virtual int create(const char* path, uint64_t* db_handle) = 0;
  virtual int close(uint64_t db_handle) = 0;

  virtual int exec_sql(uint64_t db_handle, const char* sql) = 0;

  virtual int open_table(uint64_t db_handle, const char* table_name, uint64_t* table_handle) = 0;
  virtual int close_table(uint64_t db_handle, uint64_t table_handle) = 0;

  virtual int search(uint64_t table_handle, const char* fields, const char* where_clause, uint64_t* cursor_handle) = 0;
  virtual int fetch_row(uint64_t cursor_handle, uint64_t* row_handle) = 0;
  virtual int close_cursor(uint64_t cursor_handle) = 0;

  virtual int create_row(uint64_t table_handle, uint64_t* row_handle) = 0;
  virtual int insert(uint64_t table_handle, uint64_t row_handle) = 0;
  virtual int update(uint64_t table_handle, uint64_t row_handle) = 0;
  virtual int close_row(uint64_t row_handle) = 0;

  virtual int get_field_info(uint64_t table_handle, uint64_t* field_info_handle) = 0;
  virtual int close_field_info(uint64_t field_info_handle) = 0;
  virtual int field_info_count(uint64_t field_info_handle, int32_t* out_count) = 0;
  virtual int field_info_name(uint64_t field_info_handle, int32_t index, char** out_name) = 0;

  virtual int set_string(uint64_t row_handle, const char* column_name, const char* value) = 0;
  virtual int set_int32(uint64_t row_handle, const char* column_name, int32_t value) = 0;
  virtual int set_double(uint64_t row_handle, const char* column_name, double value) = 0;
  virtual int set_blob(uint64_t row_handle, const char* column_name, const uint8_t* data, int32_t size) = 0;
  virtual int set_geometry(uint64_t row_handle, const uint8_t* wkb, int32_t size) = 0;
  virtual int set_null(uint64_t row_handle, const char* column_name) = 0;

  virtual int list_domains(uint64_t db_handle, uint64_t* cursor_handle) = 0;
  virtual int create_coded_domain(uint64_t db_handle, const char* domain_name, const char* field_type) = 0;
  virtual int add_coded_value(uint64_t db_handle, const char* domain_name, const char* code, const char* label) = 0;
  virtual int assign_domain_to_field(uint64_t db_handle, const char* table_name, const char* column_name, const char* domain_name) = 0;

  virtual int list_relationships(uint64_t db_handle, uint64_t* cursor_handle) = 0;
  virtual int create_relationship_class(
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
      int32_t is_attributed) = 0;

  virtual int list_domains_text(uint64_t db_handle, char** out_text) = 0;
  virtual int list_relationships_text(uint64_t db_handle, char** out_text) = 0;
  virtual int list_tables_text(uint64_t db_handle, char** out_text) = 0;
  virtual int row_get_string(uint64_t row_handle, const char* column_name, char** out_value) = 0;
  virtual int row_is_null(uint64_t row_handle, const char* column_name, int32_t* out_is_null) = 0;
  virtual int row_get_int32(uint64_t row_handle, const char* column_name, int32_t* out_value) = 0;
  virtual int row_get_double(uint64_t row_handle, const char* column_name, double* out_value) = 0;
  virtual int row_get_blob(uint64_t row_handle, const char* column_name, uint8_t** out_data, int32_t* out_size) = 0;
  virtual int row_get_geometry(uint64_t row_handle, uint8_t** out_wkb, int32_t* out_size) = 0;
};

std::unique_ptr<OpenFgdbBackend> create_adapter_backend();
std::unique_ptr<OpenFgdbBackend> create_gdal_backend();

class BackendDispatch {
 public:
  static BackendDispatch& instance();

  int open(const char* path, uint64_t* db_handle);
  int create(const char* path, uint64_t* db_handle);
  int close(uint64_t db_handle);
  int exec_sql(uint64_t db_handle, const char* sql);
  int open_table(uint64_t db_handle, const char* table_name, uint64_t* table_handle);
  int close_table(uint64_t db_handle, uint64_t table_handle);
  int search(uint64_t table_handle, const char* fields, const char* where_clause, uint64_t* cursor_handle);
  int fetch_row(uint64_t cursor_handle, uint64_t* row_handle);
  int close_cursor(uint64_t cursor_handle);
  int create_row(uint64_t table_handle, uint64_t* row_handle);
  int insert(uint64_t table_handle, uint64_t row_handle);
  int update(uint64_t table_handle, uint64_t row_handle);
  int close_row(uint64_t row_handle);
  int get_field_info(uint64_t table_handle, uint64_t* field_info_handle);
  int close_field_info(uint64_t field_info_handle);
  int field_info_count(uint64_t field_info_handle, int32_t* out_count);
  int field_info_name(uint64_t field_info_handle, int32_t index, char** out_name);
  int set_string(uint64_t row_handle, const char* column_name, const char* value);
  int set_int32(uint64_t row_handle, const char* column_name, int32_t value);
  int set_double(uint64_t row_handle, const char* column_name, double value);
  int set_blob(uint64_t row_handle, const char* column_name, const uint8_t* data, int32_t size);
  int set_geometry(uint64_t row_handle, const uint8_t* wkb, int32_t size);
  int set_null(uint64_t row_handle, const char* column_name);
  int list_domains(uint64_t db_handle, uint64_t* cursor_handle);
  int create_coded_domain(uint64_t db_handle, const char* domain_name, const char* field_type);
  int add_coded_value(uint64_t db_handle, const char* domain_name, const char* code, const char* label);
  int assign_domain_to_field(uint64_t db_handle, const char* table_name, const char* column_name, const char* domain_name);
  int list_relationships(uint64_t db_handle, uint64_t* cursor_handle);
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
      int32_t is_attributed);
  int list_domains_text(uint64_t db_handle, char** out_text);
  int list_relationships_text(uint64_t db_handle, char** out_text);
  int list_tables_text(uint64_t db_handle, char** out_text);
  int row_get_string(uint64_t row_handle, const char* column_name, char** out_value);
  int row_is_null(uint64_t row_handle, const char* column_name, int32_t* out_is_null);
  int row_get_int32(uint64_t row_handle, const char* column_name, int32_t* out_value);
  int row_get_double(uint64_t row_handle, const char* column_name, double* out_value);
  int row_get_blob(uint64_t row_handle, const char* column_name, uint8_t** out_data, int32_t* out_size);
  int row_get_geometry(uint64_t row_handle, uint8_t** out_wkb, int32_t* out_size);
  int list_runtime_info_text(char** out_text);

  const char* last_error_message() const;

 private:
  BackendDispatch();
  int ensure_backend_selected(const char* operation);
  int fail_local(int code, const std::string& message);
  int invoke_and_capture(int code_from_backend);
  OpenFgdbBackend* backend();
  const OpenFgdbBackend* backend() const;

  std::unique_ptr<OpenFgdbBackend> backend_;
  std::string last_error_;
};

}  // namespace openfgdb

#endif
