#ifndef OPENFGDB_NATIVE_ADAPTER_HPP
#define OPENFGDB_NATIVE_ADAPTER_HPP

#include <cstdint>
#include <string>

namespace openfgdb {

class NativeAdapter {
 public:
  static NativeAdapter& instance();

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

  const char* last_error_message() const;

 private:
  NativeAdapter();
  NativeAdapter(const NativeAdapter&) = delete;
  NativeAdapter& operator=(const NativeAdapter&) = delete;

  int fail(int code, const std::string& message);
};

}  // namespace openfgdb

#endif
