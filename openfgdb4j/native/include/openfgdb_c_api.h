#ifndef OPENFGDB_C_API_H
#define OPENFGDB_C_API_H

#include <stdint.h>

#if defined(_WIN32) && !defined(OFGDB_STATIC)
#  if defined(OFGDB_BUILD_DLL)
#    define OFGDB_API __declspec(dllexport)
#  else
#    define OFGDB_API __declspec(dllimport)
#  endif
#else
#  define OFGDB_API
#endif

#ifdef __cplusplus
extern "C" {
#endif

#define OFGDB_OK 0
#define OFGDB_ERR_INVALID_ARG 1
#define OFGDB_ERR_NOT_FOUND 2
#define OFGDB_ERR_INTERNAL 3
#define OFGDB_ERR_ALREADY_EXISTS 4

OFGDB_API int ofgdb_open(const char *path, uint64_t *db_handle);
OFGDB_API int ofgdb_create(const char *path, uint64_t *db_handle);
OFGDB_API int ofgdb_close(uint64_t db_handle);

OFGDB_API int ofgdb_exec_sql(uint64_t db_handle, const char *sql);

OFGDB_API int ofgdb_open_table(uint64_t db_handle, const char *table_name, uint64_t *table_handle);
OFGDB_API int ofgdb_close_table(uint64_t db_handle, uint64_t table_handle);

OFGDB_API int ofgdb_search(uint64_t table_handle, const char *fields, const char *where_clause, uint64_t *cursor_handle);
OFGDB_API int ofgdb_fetch_row(uint64_t cursor_handle, uint64_t *row_handle);
OFGDB_API int ofgdb_close_cursor(uint64_t cursor_handle);

OFGDB_API int ofgdb_create_row(uint64_t table_handle, uint64_t *row_handle);
OFGDB_API int ofgdb_insert(uint64_t table_handle, uint64_t row_handle);
OFGDB_API int ofgdb_update(uint64_t table_handle, uint64_t row_handle);
OFGDB_API int ofgdb_close_row(uint64_t row_handle);

OFGDB_API int ofgdb_get_field_info(uint64_t table_handle, uint64_t *field_info_handle);
OFGDB_API int ofgdb_close_field_info(uint64_t field_info_handle);
OFGDB_API int ofgdb_field_info_count(uint64_t field_info_handle, int32_t *out_count);
OFGDB_API int ofgdb_field_info_name(uint64_t field_info_handle, int32_t index, char **out_name);

OFGDB_API int ofgdb_set_string(uint64_t row_handle, const char *column_name, const char *value);
OFGDB_API int ofgdb_set_int32(uint64_t row_handle, const char *column_name, int32_t value);
OFGDB_API int ofgdb_set_double(uint64_t row_handle, const char *column_name, double value);
OFGDB_API int ofgdb_set_blob(uint64_t row_handle, const char *column_name, const uint8_t *data, int32_t size);
OFGDB_API int ofgdb_set_geometry(uint64_t row_handle, const uint8_t *wkb, int32_t size);
OFGDB_API int ofgdb_set_null(uint64_t row_handle, const char *column_name);

OFGDB_API int ofgdb_list_domains(uint64_t db_handle, uint64_t *cursor_handle);
OFGDB_API int ofgdb_create_coded_domain(uint64_t db_handle, const char *domain_name, const char *field_type);
OFGDB_API int ofgdb_add_coded_value(uint64_t db_handle, const char *domain_name, const char *code, const char *label);
OFGDB_API int ofgdb_assign_domain_to_field(uint64_t db_handle, const char *table_name, const char *column_name, const char *domain_name);

OFGDB_API int ofgdb_list_relationships(uint64_t db_handle, uint64_t *cursor_handle);
OFGDB_API int ofgdb_create_relationship_class(
    uint64_t db_handle,
    const char *name,
    const char *origin_table,
    const char *destination_table,
    const char *origin_pk,
    const char *origin_fk,
    const char *forward_label,
    const char *backward_label,
    const char *cardinality,
    int32_t is_composite,
    int32_t is_attributed
);

/* Convenience string APIs for Java FFM wrappers. The returned heap strings
 * are owned by the caller and must be freed with ofgdb_free_string().
 */
OFGDB_API int ofgdb_list_domains_text(uint64_t db_handle, char **out_text);
OFGDB_API int ofgdb_list_relationships_text(uint64_t db_handle, char **out_text);
OFGDB_API int ofgdb_list_tables_text(uint64_t db_handle, char **out_text);
OFGDB_API int ofgdb_list_runtime_info_text(char **out_text);
OFGDB_API int ofgdb_row_get_string(uint64_t row_handle, const char *column_name, char **out_value);
OFGDB_API int ofgdb_row_is_null(uint64_t row_handle, const char *column_name, int32_t *out_is_null);
OFGDB_API int ofgdb_row_get_int32(uint64_t row_handle, const char *column_name, int32_t *out_value);
OFGDB_API int ofgdb_row_get_double(uint64_t row_handle, const char *column_name, double *out_value);
OFGDB_API int ofgdb_row_get_blob(uint64_t row_handle, const char *column_name, uint8_t **out_data, int32_t *out_size);
OFGDB_API int ofgdb_row_get_geometry(uint64_t row_handle, uint8_t **out_wkb, int32_t *out_size);

OFGDB_API const char *ofgdb_last_error_message(void);
OFGDB_API void ofgdb_free_string(char *value);

#ifdef __cplusplus
}
#endif

#endif
