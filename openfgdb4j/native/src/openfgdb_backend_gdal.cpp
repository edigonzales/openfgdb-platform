#include "openfgdb_backend.hpp"

#include "openfgdb_c_api.h"

#include "cpl_conv.h"
#include "cpl_error.h"
#include "cpl_string.h"
#include "gdal.h"
#include "ogr_api.h"
#include "ogr_core.h"
#include "ogr_srs_api.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

std::string to_lower_copy(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
    return static_cast<char>(std::tolower(c));
  });
  return value;
}

std::string to_upper_copy(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
    return static_cast<char>(std::toupper(c));
  });
  return value;
}

bool env_true(const char* value) {
  if (value == nullptr) {
    return false;
  }
  std::string normalized = to_lower_copy(value);
  return normalized == "1" || normalized == "true" || normalized == "yes";
}

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

std::vector<std::string> split_ws_tokens(const std::string& in) {
  std::vector<std::string> out;
  std::istringstream stream(in);
  std::string token;
  while (stream >> token) {
    out.push_back(token);
  }
  return out;
}

std::vector<std::string> split_sql_values(const std::string& in) {
  std::vector<std::string> out;
  std::string cur;
  bool in_quote = false;
  for (size_t i = 0; i < in.size(); i++) {
    char c = in[i];
    if (c == '\'') {
      cur.push_back(c);
      if (in_quote && i + 1 < in.size() && in[i + 1] == '\'') {
        cur.push_back(in[i + 1]);
        i++;
      } else {
        in_quote = !in_quote;
      }
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

std::vector<std::string> split_sql_top_level(const std::string& in) {
  std::vector<std::string> out;
  std::string cur;
  int paren_depth = 0;
  bool in_quote = false;
  for (size_t i = 0; i < in.size(); i++) {
    char c = in[i];
    if (c == '\'') {
      cur.push_back(c);
      if (in_quote && i + 1 < in.size() && in[i + 1] == '\'') {
        cur.push_back(in[i + 1]);
        i++;
      } else {
        in_quote = !in_quote;
      }
      continue;
    }
    if (!in_quote) {
      if (c == '(') {
        paren_depth++;
      } else if (c == ')' && paren_depth > 0) {
        paren_depth--;
      } else if (c == ',' && paren_depth == 0) {
        out.push_back(trim(cur));
        cur.clear();
        continue;
      }
    }
    cur.push_back(c);
  }
  if (!cur.empty()) {
    out.push_back(trim(cur));
  }
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

char* dup_cstr(const std::string& value) {
  char* out = static_cast<char*>(std::malloc(value.size() + 1));
  if (out == nullptr) {
    return nullptr;
  }
  std::memcpy(out, value.c_str(), value.size() + 1);
  return out;
}

uint8_t* dup_bytes(const uint8_t* value, int32_t size) {
  if (size <= 0 || value == nullptr) {
    return nullptr;
  }
  uint8_t* out = static_cast<uint8_t*>(std::malloc(static_cast<size_t>(size)));
  if (out == nullptr) {
    return nullptr;
  }
  std::memcpy(out, value, static_cast<size_t>(size));
  return out;
}

std::string join_lines(const std::vector<std::string>& values) {
  std::ostringstream out;
  for (size_t i = 0; i < values.size(); i++) {
    if (i > 0) {
      out << '\n';
    }
    out << values[i];
  }
  return out.str();
}

std::string quote_identifier(const std::string& name) {
  std::string out = "\"";
  for (char c : name) {
    if (c == '"') {
      out += "\"\"";
    } else {
      out.push_back(c);
    }
  }
  out.push_back('"');
  return out;
}

bool equals_ci(const std::string& lhs, const std::string& rhs) {
  return to_upper_copy(lhs) == to_upper_copy(rhs);
}

static const std::string kByteLiteralPrefix = "__OFGDB_BYTES_B64__:";
static const char* kGeomContractColMetadataKey = "OPENFGDB_CONTRACT_COL";
static const char* kGeomContractKindMetadataKey = "OPENFGDB_CONTRACT_KIND";
static const char* kGeomContractRegistryTable = "__openfgdb_geom_contract";
static const char* kGeomContractRegistryTableField = "table_name";
static const char* kGeomContractRegistryColumnField = "declared_col";
static const char* kGeomContractRegistryKindField = "contract_kind";

bool is_internal_table_name(const std::string& table_name) {
  return equals_ci(trim(table_name), kGeomContractRegistryTable);
}

std::string escape_sql_literal(const std::string& value) {
  std::string escaped;
  escaped.reserve(value.size() + 8);
  for (char c : value) {
    if (c == '\'') {
      escaped += "''";
    } else {
      escaped.push_back(c);
    }
  }
  return escaped;
}

bool contains_ci(const std::string& text, const std::string& needle) {
  if (needle.empty()) {
    return true;
  }
  return to_lower_copy(text).find(to_lower_copy(needle)) != std::string::npos;
}

bool starts_with_ci(const std::string& text, const char* prefix) {
  if (prefix == nullptr) {
    return false;
  }
  std::string norm_text = to_upper_copy(trim(text));
  std::string norm_prefix = to_upper_copy(prefix);
  return norm_text.rfind(norm_prefix, 0) == 0;
}

int map_ogr_error(OGRErr err) {
  if (err == OGRERR_NONE) {
    return OFGDB_OK;
  }
  if (err == OGRERR_NON_EXISTING_FEATURE) {
    return OFGDB_ERR_NOT_FOUND;
  }
  return OFGDB_ERR_INTERNAL;
}

const char* ogr_field_type_name_safe(OGRFieldType type) {
  const char* name = OGR_GetFieldTypeName(type);
  return (name != nullptr && *name != '\0') ? name : "UNKNOWN";
}

OGRFieldType map_field_type_from_string(const std::string& in) {
  std::string type = to_upper_copy(in);
  if (contains_ci(type, "BIGINT") || contains_ci(type, "INT64")) {
    return OFTInteger64;
  }
  if (contains_ci(type, "INT")) {
    return OFTInteger;
  }
  if (contains_ci(type, "REAL") || contains_ci(type, "DOUBLE") || contains_ci(type, "FLOAT") || contains_ci(type, "DECIMAL") ||
      contains_ci(type, "NUMERIC")) {
    return OFTReal;
  }
  if (contains_ci(type, "BLOB") || contains_ci(type, "BINARY")) {
    return OFTBinary;
  }
  if (contains_ci(type, "DATE") || contains_ci(type, "TIME")) {
    return OFTDateTime;
  }
  return OFTString;
}

OGRFieldType map_field_type_from_symbolic_name(const std::string& in) {
  std::string type = to_upper_copy(in);
  if (type == "INTEGER" || type == "INT") {
    return OFTInteger;
  }
  if (type == "STRING" || type == "TEXT") {
    return OFTString;
  }
  if (type == "DOUBLE" || type == "REAL") {
    return OFTReal;
  }
  if (type == "BLOB" || type == "BINARY") {
    return OFTBinary;
  }
  return OFTString;
}

std::string map_field_type_to_symbolic_name(OGRFieldType type) {
  switch (type) {
    case OFTInteger64:
      return "INTEGER";
    case OFTInteger:
      return "INTEGER";
    case OFTReal:
      return "DOUBLE";
    case OFTBinary:
      return "BLOB";
    default:
      return "STRING";
  }
}

int find_field_index_ci(OGRFeatureDefnH defn, const char* field_name) {
  if (defn == nullptr || field_name == nullptr) {
    return -1;
  }
  int exact = OGR_FD_GetFieldIndex(defn, field_name);
  if (exact >= 0) {
    return exact;
  }
  std::string wanted = to_upper_copy(field_name);
  int count = OGR_FD_GetFieldCount(defn);
  for (int i = 0; i < count; i++) {
    OGRFieldDefnH fld = OGR_FD_GetFieldDefn(defn, i);
    if (fld == nullptr) {
      continue;
    }
    const char* name = OGR_Fld_GetNameRef(fld);
    if (name != nullptr && to_upper_copy(name) == wanted) {
      return i;
    }
  }
  return -1;
}

int find_geom_field_index_ci(OGRFeatureDefnH defn, const char* field_name) {
  if (defn == nullptr || field_name == nullptr) {
    return -1;
  }
  std::string wanted = to_upper_copy(field_name);
  int count = OGR_FD_GetGeomFieldCount(defn);
  for (int i = 0; i < count; i++) {
    OGRGeomFieldDefnH geom = OGR_FD_GetGeomFieldDefn(defn, i);
    if (geom == nullptr) {
      continue;
    }
    const char* name = OGR_GFld_GetNameRef(geom);
    if (name != nullptr && to_upper_copy(name) == wanted) {
      return i;
    }
  }
  return -1;
}

enum class GeometryContractKind {
  kUnknown,
  kPoint,
  kMultiPoint,
  kLine,
  kCircularString,
  kCompoundCurve,
  kMultiLine,
  kMultiCurve,
  kPolygon,
  kCurvePolygon,
  kMultiPolygon,
  kMultiSurface,
};

const char* geometry_contract_kind_name(GeometryContractKind kind) {
  switch (kind) {
    case GeometryContractKind::kPoint:
      return "POINT";
    case GeometryContractKind::kMultiPoint:
      return "MULTIPOINT";
    case GeometryContractKind::kLine:
      return "LINE";
    case GeometryContractKind::kCircularString:
      return "CIRCULARSTRING";
    case GeometryContractKind::kCompoundCurve:
      return "COMPOUNDCURVE";
    case GeometryContractKind::kMultiLine:
      return "MULTILINE";
    case GeometryContractKind::kMultiCurve:
      return "MULTICURVE";
    case GeometryContractKind::kPolygon:
      return "POLYGON";
    case GeometryContractKind::kCurvePolygon:
      return "CURVEPOLYGON";
    case GeometryContractKind::kMultiPolygon:
      return "MULTIPOLYGON";
    case GeometryContractKind::kMultiSurface:
      return "MULTISURFACE";
    default:
      return "UNKNOWN";
  }
}

GeometryContractKind parse_geometry_contract_kind(const std::string& kind_raw) {
  std::string kind = to_upper_copy(trim(kind_raw));
  if (kind == "POINT") {
    return GeometryContractKind::kPoint;
  }
  if (kind == "MULTIPOINT") {
    return GeometryContractKind::kMultiPoint;
  }
  if (kind == "LINE" || kind == "LINESTRING") {
    return GeometryContractKind::kLine;
  }
  if (kind == "CIRCULARSTRING") {
    return GeometryContractKind::kCircularString;
  }
  if (kind == "COMPOUNDCURVE") {
    return GeometryContractKind::kCompoundCurve;
  }
  if (kind == "MULTILINE" || kind == "MULTILINESTRING") {
    return GeometryContractKind::kMultiLine;
  }
  if (kind == "MULTICURVE") {
    return GeometryContractKind::kMultiCurve;
  }
  if (kind == "POLYGON") {
    return GeometryContractKind::kPolygon;
  }
  if (kind == "CURVEPOLYGON") {
    return GeometryContractKind::kCurvePolygon;
  }
  if (kind == "MULTIPOLYGON") {
    return GeometryContractKind::kMultiPolygon;
  }
  if (kind == "MULTISURFACE") {
    return GeometryContractKind::kMultiSurface;
  }
  return GeometryContractKind::kUnknown;
}

GeometryContractKind map_ogr_geometry_to_contract_kind(OGRwkbGeometryType type) {
  switch (wkbFlatten(type)) {
    case wkbPoint:
      return GeometryContractKind::kPoint;
    case wkbMultiPoint:
      return GeometryContractKind::kMultiPoint;
    case wkbLineString:
      return GeometryContractKind::kLine;
    case wkbCircularString:
      return GeometryContractKind::kCircularString;
    case wkbCompoundCurve:
      return GeometryContractKind::kCompoundCurve;
    case wkbMultiLineString:
      return GeometryContractKind::kMultiLine;
    case wkbMultiCurve:
      return GeometryContractKind::kMultiCurve;
    case wkbPolygon:
      return GeometryContractKind::kPolygon;
    case wkbCurvePolygon:
      return GeometryContractKind::kCurvePolygon;
    case wkbMultiPolygon:
      return GeometryContractKind::kMultiPolygon;
    case wkbMultiSurface:
      return GeometryContractKind::kMultiSurface;
    default:
      return GeometryContractKind::kUnknown;
  }
}

OGRwkbGeometryType map_contract_kind_to_ogr_base(GeometryContractKind kind) {
  switch (kind) {
    case GeometryContractKind::kPoint:
      return wkbPoint;
    case GeometryContractKind::kMultiPoint:
      return wkbMultiPoint;
    case GeometryContractKind::kLine:
      return wkbLineString;
    case GeometryContractKind::kCircularString:
      return wkbCircularString;
    case GeometryContractKind::kCompoundCurve:
      return wkbCompoundCurve;
    case GeometryContractKind::kMultiLine:
      return wkbMultiLineString;
    case GeometryContractKind::kMultiCurve:
      return wkbMultiCurve;
    case GeometryContractKind::kPolygon:
      return wkbPolygon;
    case GeometryContractKind::kCurvePolygon:
      return wkbCurvePolygon;
    case GeometryContractKind::kMultiPolygon:
      return wkbMultiPolygon;
    case GeometryContractKind::kMultiSurface:
      return wkbMultiSurface;
    default:
      return wkbUnknown;
  }
}

GeometryContractKind single_kind_for_multi(GeometryContractKind kind) {
  if (kind == GeometryContractKind::kMultiPoint) {
    return GeometryContractKind::kPoint;
  }
  if (kind == GeometryContractKind::kMultiLine) {
    return GeometryContractKind::kLine;
  }
  if (kind == GeometryContractKind::kMultiCurve) {
    return GeometryContractKind::kCompoundCurve;
  }
  if (kind == GeometryContractKind::kMultiPolygon) {
    return GeometryContractKind::kPolygon;
  }
  if (kind == GeometryContractKind::kMultiSurface) {
    return GeometryContractKind::kCurvePolygon;
  }
  return GeometryContractKind::kUnknown;
}

bool is_multi_kind(GeometryContractKind kind) {
  return kind == GeometryContractKind::kMultiPoint || kind == GeometryContractKind::kMultiLine ||
         kind == GeometryContractKind::kMultiCurve || kind == GeometryContractKind::kMultiPolygon ||
         kind == GeometryContractKind::kMultiSurface;
}

struct GeometrySqlColumnSpec {
  std::string name;
  GeometryContractKind contract_kind = GeometryContractKind::kUnknown;
  OGRwkbGeometryType ogr_type = wkbUnknown;
  int epsg = 0;
  bool nullable = true;
};

bool parse_int32_strict(const std::string& in, int* out) {
  if (out == nullptr) {
    return false;
  }
  try {
    size_t consumed = 0;
    long value = std::stol(trim(in), &consumed, 10);
    if (consumed != trim(in).size()) {
      return false;
    }
    if (value < std::numeric_limits<int>::min() || value > std::numeric_limits<int>::max()) {
      return false;
    }
    *out = static_cast<int>(value);
    return true;
  } catch (const std::exception&) {
    return false;
  }
}

bool decode_byte_literal_value(const std::string& in, std::vector<GByte>* out) {
  if (out == nullptr) {
    return false;
  }
  out->clear();
  if (in.rfind(kByteLiteralPrefix, 0) != 0) {
    return false;
  }
  std::string encoded = in.substr(kByteLiteralPrefix.size());
  std::vector<GByte> decoded(encoded.begin(), encoded.end());
  decoded.push_back('\0');
  int decoded_size = CPLBase64DecodeInPlace(decoded.data());
  if (decoded_size < 0) {
    return false;
  }
  decoded.resize(static_cast<size_t>(decoded_size));
  *out = std::move(decoded);
  return true;
}

OGRwkbGeometryType map_geometry_kind(const std::string& kind_raw, int dim, GeometryContractKind* out_kind) {
  GeometryContractKind kind = parse_geometry_contract_kind(kind_raw);
  OGRwkbGeometryType base = map_contract_kind_to_ogr_base(kind);
  if (base == wkbUnknown) {
    return wkbUnknown;
  }
  if (out_kind != nullptr) {
    *out_kind = kind;
  }
  if (dim == 3) {
    return wkbSetZ(base);
  }
  if (dim == 2) {
    return base;
  }
  return wkbUnknown;
}

GeometryContractKind get_declared_geom_kind(OGRFeatureH feat, int geom_idx) {
  if (feat == nullptr || geom_idx < 0) {
    return GeometryContractKind::kUnknown;
  }
  OGRFeatureDefnH defn = OGR_F_GetDefnRef(feat);
  if (defn == nullptr || geom_idx >= OGR_FD_GetGeomFieldCount(defn)) {
    return GeometryContractKind::kUnknown;
  }
  OGRGeomFieldDefnH geom_defn = OGR_FD_GetGeomFieldDefn(defn, geom_idx);
  if (geom_defn == nullptr) {
    return GeometryContractKind::kUnknown;
  }
  return map_ogr_geometry_to_contract_kind(OGR_GFld_GetType(geom_defn));
}

bool check_geometry_contract_kind(GeometryContractKind declared_kind, OGRGeometryH geom, std::string* out_error) {
  if (out_error != nullptr) {
    out_error->clear();
  }
  if (geom == nullptr) {
    return true;
  }
  if (declared_kind == GeometryContractKind::kUnknown) {
    return true;
  }
  GeometryContractKind actual_kind = map_ogr_geometry_to_contract_kind(OGR_G_GetGeometryType(geom));
  if (declared_kind == actual_kind) {
    return true;
  }
  if (out_error != nullptr) {
    *out_error = "geometry type mismatch: expected ";
    *out_error += geometry_contract_kind_name(declared_kind);
    *out_error += ", got ";
    *out_error += geometry_contract_kind_name(actual_kind);
  }
  return false;
}

OGRGeometryH clone_curve_ring_for_curvepolygon(OGRGeometryH ring, std::string* out_error) {
  if (out_error != nullptr) {
    out_error->clear();
  }
  if (ring == nullptr) {
    if (out_error != nullptr) {
      *out_error = "missing CURVEPOLYGON ring geometry";
    }
    return nullptr;
  }
  const char* ring_name_raw = OGR_G_GetGeometryName(ring);
  std::string ring_name_upper = to_upper_copy(ring_name_raw != nullptr ? ring_name_raw : "");
  if (ring_name_upper.rfind("LINEARRING", 0) == 0) {
    OGRwkbGeometryType ring_type = OGR_G_GetGeometryType(ring);
    OGRwkbGeometryType target_type = OGR_GT_HasZ(ring_type) ? wkbSetZ(wkbLineString) : wkbLineString;
    OGRGeometryH as_line = OGR_G_CreateGeometry(target_type);
    if (as_line == nullptr) {
      if (out_error != nullptr) {
        *out_error = "failed to allocate LINESTRING for LINEARRING conversion";
      }
      return nullptr;
    }
    int point_count = OGR_G_GetPointCount(ring);
    for (int i = 0; i < point_count; i++) {
      double x = OGR_G_GetX(ring, i);
      double y = OGR_G_GetY(ring, i);
      if (OGR_GT_HasZ(ring_type)) {
        OGR_G_AddPoint(as_line, x, y, OGR_G_GetZ(ring, i));
      } else {
        OGR_G_AddPoint_2D(as_line, x, y);
      }
    }
    return as_line;
  }
  if (ring_name_upper.rfind("CIRCULARSTRING", 0) == 0 || ring_name_upper.rfind("COMPOUNDCURVE", 0) == 0 ||
      ring_name_upper == "CURVE" || ring_name_upper.rfind("CURVEPOLYGON", 0) == 0) {
    OGRGeometryH clone = OGR_G_Clone(ring);
    if (clone == nullptr && out_error != nullptr) {
      *out_error = "failed to clone curve ring geometry";
    }
    return clone;
  }
  if (ring_name_upper.rfind("MULTILINESTRING", 0) == 0 || ring_name_upper.rfind("MULTICURVE", 0) == 0) {
    int part_count = OGR_G_GetGeometryCount(ring);
    if (part_count != 1) {
      if (out_error != nullptr) {
        *out_error = "unsupported CURVEPOLYGON ring collection with ";
        *out_error += std::to_string(part_count);
        *out_error += " parts";
      }
      return nullptr;
    }
    OGRGeometryH single_part = OGR_G_GetGeometryRef(ring, 0);
    if (single_part == nullptr) {
      if (out_error != nullptr) {
        *out_error = "failed to access single ring part";
      }
      return nullptr;
    }
    return clone_curve_ring_for_curvepolygon(single_part, out_error);
  }
  OGRGeometryH clone = OGR_G_Clone(ring);
  if (clone == nullptr && out_error != nullptr) {
    *out_error = "failed to clone CURVEPOLYGON ring geometry";
  }
  return clone;
}

OGRGeometryH curvepolygon_from_polygon_like(OGRGeometryH polygon_like, std::string* out_error) {
  if (out_error != nullptr) {
    out_error->clear();
  }
  if (polygon_like == nullptr) {
    if (out_error != nullptr) {
      *out_error = "missing polygon geometry";
    }
    return nullptr;
  }
  const char* source_name = OGR_G_GetGeometryName(polygon_like);
  std::string source_name_upper = to_upper_copy(source_name != nullptr ? source_name : "");
  if (source_name_upper.rfind("CURVEPOLYGON", 0) == 0) {
    OGRGeometryH clone = OGR_G_Clone(polygon_like);
    if (clone == nullptr && out_error != nullptr) {
      *out_error = "failed to clone CURVEPOLYGON geometry";
    }
    return clone;
  }
  OGRwkbGeometryType source_type = OGR_G_GetGeometryType(polygon_like);
  OGRwkbGeometryType target_type = OGR_GT_HasZ(source_type) ? wkbSetZ(wkbCurvePolygon) : wkbCurvePolygon;
  OGRGeometryH curve_polygon = OGR_G_CreateGeometry(target_type);
  if (curve_polygon == nullptr) {
    if (out_error != nullptr) {
      *out_error = "failed to allocate CURVEPOLYGON";
    }
    return nullptr;
  }
  int ring_count = OGR_G_GetGeometryCount(polygon_like);
  for (int i = 0; i < ring_count; i++) {
    OGRGeometryH ring = OGR_G_GetGeometryRef(polygon_like, i);
    if (ring == nullptr) {
      OGR_G_DestroyGeometry(curve_polygon);
      if (out_error != nullptr) {
        *out_error = "failed to access polygon ring (declared=CURVEPOLYGON, ring_index=" + std::to_string(i) + ")";
      }
      return nullptr;
    }
    std::string ring_error;
    OGRGeometryH ring_curve = clone_curve_ring_for_curvepolygon(ring, &ring_error);
    if (ring_curve == nullptr) {
      OGR_G_DestroyGeometry(curve_polygon);
      if (out_error != nullptr) {
        *out_error = "failed to normalize CURVEPOLYGON ring (declared=CURVEPOLYGON, ring_index=" + std::to_string(i) +
                     "): " + ring_error;
      }
      return nullptr;
    }
    OGRErr err = OGR_G_AddGeometry(curve_polygon, ring_curve);
    OGR_G_DestroyGeometry(ring_curve);
    if (err != OGRERR_NONE) {
      // Some providers expose polygon rings as linear-ring specific types.
      // Retry with GDAL's linear geometry conversion if direct add failed.
      OGRGeometryH ring_linear = OGR_G_GetLinearGeometry(ring, 0.0, nullptr);
      if (ring_linear != nullptr) {
        OGRErr linear_err = OGR_G_AddGeometry(curve_polygon, ring_linear);
        OGR_G_DestroyGeometry(ring_linear);
        if (linear_err == OGRERR_NONE) {
          continue;
        }
      }
      OGR_G_DestroyGeometry(curve_polygon);
      if (out_error != nullptr) {
        const char* ring_name = OGR_G_GetGeometryName(ring);
        const char* cpl_msg = CPLGetLastErrorMsg();
        *out_error = "failed to add ring to CURVEPOLYGON (declared=CURVEPOLYGON, ring_index=" + std::to_string(i) +
                     ", ring_type=" + std::string(ring_name != nullptr ? ring_name : "UNKNOWN") + ")";
        if (cpl_msg != nullptr && std::strlen(cpl_msg) > 0) {
          *out_error += ": ";
          *out_error += cpl_msg;
        }
      }
      return nullptr;
    }
  }
  return curve_polygon;
}

OGRGeometryH promote_curve_geometry_for_read(GeometryContractKind declared_kind, GeometryContractKind actual_kind,
                                             OGRGeometryH geom, std::string* out_error) {
  if (out_error != nullptr) {
    out_error->clear();
  }
  if (geom == nullptr) {
    return nullptr;
  }

  if (declared_kind == GeometryContractKind::kCompoundCurve) {
    OGRGeometryH single_curve = geom;
    GeometryContractKind single_kind = actual_kind;
    if (single_kind == GeometryContractKind::kMultiLine || single_kind == GeometryContractKind::kMultiCurve) {
      int part_count = OGR_G_GetGeometryCount(geom);
      if (part_count != 1) {
        if (out_error != nullptr) {
          *out_error = "geometry contract violation: expected COMPOUNDCURVE with exactly one part";
        }
        return nullptr;
      }
      single_curve = OGR_G_GetGeometryRef(geom, 0);
      if (single_curve == nullptr) {
        if (out_error != nullptr) {
          *out_error = "geometry contract violation: missing curve part";
        }
        return nullptr;
      }
      single_kind = map_ogr_geometry_to_contract_kind(OGR_G_GetGeometryType(single_curve));
    }

    // Defensive unwrap: a nested single-part MULTI* can still appear in some provider paths.
    while (single_kind == GeometryContractKind::kMultiLine || single_kind == GeometryContractKind::kMultiCurve) {
      int nested_count = OGR_G_GetGeometryCount(single_curve);
      if (nested_count != 1) {
        if (out_error != nullptr) {
          *out_error = "geometry contract violation: expected COMPOUNDCURVE but got " +
                       std::string(geometry_contract_kind_name(single_kind)) + " with " +
                       std::to_string(nested_count) + " parts";
        }
        return nullptr;
      }
      OGRGeometryH nested_part = OGR_G_GetGeometryRef(single_curve, 0);
      if (nested_part == nullptr) {
        if (out_error != nullptr) {
          *out_error = "geometry contract violation: missing nested curve part";
        }
        return nullptr;
      }
      single_curve = nested_part;
      single_kind = map_ogr_geometry_to_contract_kind(OGR_G_GetGeometryType(single_curve));
    }

    if (single_kind == GeometryContractKind::kCompoundCurve) {
      OGRGeometryH direct_clone = OGR_G_Clone(single_curve);
      if (direct_clone == nullptr) {
        if (out_error != nullptr) {
          *out_error = "failed to clone COMPOUNDCURVE";
        }
        return nullptr;
      }
      return direct_clone;
    }

    if (single_kind != GeometryContractKind::kLine && single_kind != GeometryContractKind::kCircularString) {
      if (out_error != nullptr) {
        *out_error = "geometry contract violation: expected COMPOUNDCURVE, got " +
                     std::string(geometry_contract_kind_name(single_kind));
      }
      return nullptr;
    }

    OGRwkbGeometryType source_type = OGR_G_GetGeometryType(single_curve);
    OGRwkbGeometryType target_type = OGR_GT_HasZ(source_type) ? wkbSetZ(wkbCompoundCurve) : wkbCompoundCurve;
    OGRGeometryH compound_curve = OGR_G_CreateGeometry(target_type);
    if (compound_curve == nullptr) {
      if (out_error != nullptr) {
        *out_error = "failed to allocate COMPOUNDCURVE";
      }
      return nullptr;
    }

    OGRGeometryH curve_clone = OGR_G_Clone(single_curve);
    if (curve_clone == nullptr) {
      OGR_G_DestroyGeometry(compound_curve);
      if (out_error != nullptr) {
        *out_error = "failed to clone curve part";
      }
      return nullptr;
    }
    OGRErr err = OGR_G_AddGeometry(compound_curve, curve_clone);
    OGR_G_DestroyGeometry(curve_clone);
    if (err != OGRERR_NONE) {
      OGR_G_DestroyGeometry(compound_curve);
      if (out_error != nullptr) {
        const char* geom_name = OGR_G_GetGeometryName(single_curve);
        *out_error = "failed to build COMPOUNDCURVE geometry from " +
                     std::string(geom_name != nullptr ? geom_name : "UNKNOWN");
      }
      return nullptr;
    }
    return compound_curve;
  }

  if (declared_kind == GeometryContractKind::kMultiCurve) {
    OGRwkbGeometryType source_type = OGR_G_GetGeometryType(geom);
    OGRwkbGeometryType target_type = OGR_GT_HasZ(source_type) ? wkbSetZ(wkbMultiCurve) : wkbMultiCurve;
    OGRGeometryH multi_curve = OGR_G_CreateGeometry(target_type);
    if (multi_curve == nullptr) {
      if (out_error != nullptr) {
        *out_error = "failed to allocate MULTICURVE";
      }
      return nullptr;
    }
    if (actual_kind == GeometryContractKind::kLine || actual_kind == GeometryContractKind::kCircularString ||
        actual_kind == GeometryContractKind::kCompoundCurve) {
      OGRGeometryH curve_clone = OGR_G_Clone(geom);
      if (curve_clone == nullptr) {
        OGR_G_DestroyGeometry(multi_curve);
        if (out_error != nullptr) {
          *out_error = "failed to clone curve";
        }
        return nullptr;
      }
      OGRErr err = OGR_G_AddGeometry(multi_curve, curve_clone);
      OGR_G_DestroyGeometry(curve_clone);
      if (err != OGRERR_NONE) {
        OGR_G_DestroyGeometry(multi_curve);
        if (out_error != nullptr) {
          *out_error = "failed to build MULTICURVE geometry";
        }
        return nullptr;
      }
      return multi_curve;
    }
    int part_count = OGR_G_GetGeometryCount(geom);
    for (int i = 0; i < part_count; i++) {
      OGRGeometryH part = OGR_G_GetGeometryRef(geom, i);
      if (part == nullptr) {
        OGR_G_DestroyGeometry(multi_curve);
        if (out_error != nullptr) {
          *out_error = "failed to access curve part";
        }
        return nullptr;
      }
      OGRGeometryH part_clone = OGR_G_Clone(part);
      if (part_clone == nullptr) {
        OGR_G_DestroyGeometry(multi_curve);
        if (out_error != nullptr) {
          *out_error = "failed to clone curve part";
        }
        return nullptr;
      }
      OGRErr err = OGR_G_AddGeometry(multi_curve, part_clone);
      OGR_G_DestroyGeometry(part_clone);
      if (err != OGRERR_NONE) {
        OGR_G_DestroyGeometry(multi_curve);
        if (out_error != nullptr) {
          *out_error = "failed to build MULTICURVE geometry";
        }
        return nullptr;
      }
    }
    return multi_curve;
  }

  if (declared_kind == GeometryContractKind::kCurvePolygon) {
    if (actual_kind == GeometryContractKind::kCurvePolygon) {
      return OGR_G_Clone(geom);
    }
    if (actual_kind == GeometryContractKind::kPolygon) {
      return curvepolygon_from_polygon_like(geom, out_error);
    }
    if (actual_kind == GeometryContractKind::kMultiPolygon || actual_kind == GeometryContractKind::kMultiSurface) {
      int part_count = OGR_G_GetGeometryCount(geom);
      if (part_count != 1) {
        if (out_error != nullptr) {
          *out_error = "geometry contract violation: expected CURVEPOLYGON with exactly one surface";
        }
        return nullptr;
      }
      OGRGeometryH first_surface = OGR_G_GetGeometryRef(geom, 0);
      if (first_surface == nullptr) {
        if (out_error != nullptr) {
          *out_error = "geometry contract violation: missing polygon surface";
        }
        return nullptr;
      }
      GeometryContractKind first_kind = map_ogr_geometry_to_contract_kind(OGR_G_GetGeometryType(first_surface));
      if (first_kind == GeometryContractKind::kCurvePolygon) {
        OGRGeometryH clone = OGR_G_Clone(first_surface);
        if (clone == nullptr && out_error != nullptr) {
          *out_error = "failed to clone CURVEPOLYGON surface";
        }
        return clone;
      }
      if (first_kind == GeometryContractKind::kPolygon) {
        return curvepolygon_from_polygon_like(first_surface, out_error);
      }
      if (out_error != nullptr) {
        *out_error = "geometry contract violation: expected CURVEPOLYGON surface, got ";
        *out_error += geometry_contract_kind_name(first_kind);
      }
      return nullptr;
    }
  }

  if (declared_kind == GeometryContractKind::kMultiSurface) {
    OGRwkbGeometryType source_type = OGR_G_GetGeometryType(geom);
    OGRwkbGeometryType target_type = OGR_GT_HasZ(source_type) ? wkbSetZ(wkbMultiSurface) : wkbMultiSurface;
    OGRGeometryH multi_surface = OGR_G_CreateGeometry(target_type);
    if (multi_surface == nullptr) {
      if (out_error != nullptr) {
        *out_error = "failed to allocate MULTISURFACE";
      }
      return nullptr;
    }
    if (actual_kind == GeometryContractKind::kPolygon || actual_kind == GeometryContractKind::kCurvePolygon) {
      OGRGeometryH curve_polygon = nullptr;
      if (actual_kind == GeometryContractKind::kCurvePolygon) {
        curve_polygon = OGR_G_Clone(geom);
      } else {
        curve_polygon = curvepolygon_from_polygon_like(geom, out_error);
      }
      if (curve_polygon == nullptr) {
        OGR_G_DestroyGeometry(multi_surface);
        return nullptr;
      }
      OGRErr err = OGR_G_AddGeometry(multi_surface, curve_polygon);
      OGR_G_DestroyGeometry(curve_polygon);
      if (err != OGRERR_NONE) {
        OGR_G_DestroyGeometry(multi_surface);
        if (out_error != nullptr) {
          *out_error = "failed to build MULTISURFACE geometry";
        }
        return nullptr;
      }
      return multi_surface;
    }
    int part_count = OGR_G_GetGeometryCount(geom);
    for (int i = 0; i < part_count; i++) {
      OGRGeometryH polygon_part = OGR_G_GetGeometryRef(geom, i);
      if (polygon_part == nullptr) {
        OGR_G_DestroyGeometry(multi_surface);
        if (out_error != nullptr) {
          *out_error = "failed to access polygon part";
        }
        return nullptr;
      }
      GeometryContractKind part_kind = map_ogr_geometry_to_contract_kind(OGR_G_GetGeometryType(polygon_part));
      OGRGeometryH curve_polygon = nullptr;
      if (part_kind == GeometryContractKind::kCurvePolygon) {
        curve_polygon = OGR_G_Clone(polygon_part);
      } else if (part_kind == GeometryContractKind::kPolygon) {
        curve_polygon = curvepolygon_from_polygon_like(polygon_part, out_error);
      } else {
        if (out_error != nullptr) {
          *out_error = "geometry contract violation: expected MULTISURFACE part as CURVEPOLYGON/POLYGON, got ";
          *out_error += geometry_contract_kind_name(part_kind);
        }
        OGR_G_DestroyGeometry(multi_surface);
        return nullptr;
      }
      if (curve_polygon == nullptr) {
        OGR_G_DestroyGeometry(multi_surface);
        return nullptr;
      }
      OGRErr err = OGR_G_AddGeometry(multi_surface, curve_polygon);
      OGR_G_DestroyGeometry(curve_polygon);
      if (err != OGRERR_NONE) {
        OGR_G_DestroyGeometry(multi_surface);
        if (out_error != nullptr) {
          *out_error = "failed to build MULTISURFACE geometry";
        }
        return nullptr;
      }
    }
    return multi_surface;
  }

  return nullptr;
}

bool normalize_geometry_for_read_kind(
    GeometryContractKind declared_kind,
    OGRGeometryH geom,
    OGRGeometryH* out_geom,
    std::string* out_error) {
  if (out_error != nullptr) {
    out_error->clear();
  }
  if (out_geom == nullptr) {
    return false;
  }
  *out_geom = nullptr;
  if (geom == nullptr) {
    return true;
  }

  GeometryContractKind actual_kind = map_ogr_geometry_to_contract_kind(OGR_G_GetGeometryType(geom));
  if (declared_kind == GeometryContractKind::kUnknown || declared_kind == actual_kind) {
    *out_geom = OGR_G_Clone(geom);
    if (*out_geom == nullptr && out_error != nullptr) {
      *out_error = "failed to clone geometry";
    }
    return *out_geom != nullptr;
  }

  if (declared_kind == GeometryContractKind::kCompoundCurve || declared_kind == GeometryContractKind::kMultiCurve ||
      declared_kind == GeometryContractKind::kCurvePolygon || declared_kind == GeometryContractKind::kMultiSurface) {
    OGRGeometryH promoted = promote_curve_geometry_for_read(declared_kind, actual_kind, geom, out_error);
    if (promoted != nullptr) {
      *out_geom = promoted;
      return true;
    }
    if (out_error != nullptr && out_error->empty()) {
      *out_error = "failed to normalize curve geometry";
    }
    return false;
  }

  if (is_multi_kind(declared_kind) && single_kind_for_multi(declared_kind) == actual_kind) {
    OGRwkbGeometryType base = map_contract_kind_to_ogr_base(declared_kind);
    if (base == wkbUnknown) {
      if (out_error != nullptr) {
        *out_error = "failed to resolve declared geometry type";
      }
      return false;
    }
    OGRwkbGeometryType source_type = OGR_G_GetGeometryType(geom);
    OGRwkbGeometryType target_type = OGR_GT_HasZ(source_type) ? wkbSetZ(base) : base;
    OGRGeometryH multi = OGR_G_CreateGeometry(target_type);
    if (multi == nullptr) {
      if (out_error != nullptr) {
        *out_error = "failed to allocate geometry collection";
      }
      return false;
    }
    OGRGeometryH single_clone = OGR_G_Clone(geom);
    if (single_clone == nullptr) {
      OGR_G_DestroyGeometry(multi);
      if (out_error != nullptr) {
        *out_error = "failed to clone geometry part";
      }
      return false;
    }
    OGRErr err = OGR_G_AddGeometryDirectly(multi, single_clone);
    if (err != OGRERR_NONE) {
      OGR_G_DestroyGeometry(single_clone);
      OGR_G_DestroyGeometry(multi);
      if (out_error != nullptr) {
        *out_error = "failed to wrap single geometry as multi geometry";
      }
      return false;
    }
    *out_geom = multi;
    return true;
  }

  if (!is_multi_kind(declared_kind) && is_multi_kind(actual_kind) && single_kind_for_multi(actual_kind) == declared_kind) {
    int part_count = OGR_G_GetGeometryCount(geom);
    if (part_count != 1) {
      if (out_error != nullptr) {
        *out_error = "geometry contract violation: ";
        *out_error += geometry_contract_kind_name(declared_kind);
        *out_error += " column contains ";
        *out_error += std::to_string(part_count);
        *out_error += " parts";
      }
      return false;
    }
    OGRGeometryH part = OGR_G_GetGeometryRef(geom, 0);
    if (part == nullptr) {
      if (out_error != nullptr) {
        *out_error = "geometry contract violation: missing first geometry part";
      }
      return false;
    }
    *out_geom = OGR_G_Clone(part);
    if (*out_geom == nullptr && out_error != nullptr) {
      *out_error = "failed to clone single geometry part";
    }
    return *out_geom != nullptr;
  }

  if (out_error != nullptr) {
    *out_error = "geometry contract violation: expected ";
    *out_error += geometry_contract_kind_name(declared_kind);
    *out_error += ", got ";
    *out_error += geometry_contract_kind_name(actual_kind);
  }
  return false;
}

bool normalize_geometry_for_read(
    OGRFeatureH feat,
    int geom_idx,
    OGRGeometryH geom,
    OGRGeometryH* out_geom,
    std::string* out_error) {
  GeometryContractKind declared_kind = get_declared_geom_kind(feat, geom_idx);
  return normalize_geometry_for_read_kind(declared_kind, geom, out_geom, out_error);
}

bool parse_ofgdb_geometry_type(
    const std::string& column_name,
    const std::string& type_token,
    const std::string& full_definition,
    GeometrySqlColumnSpec* out) {
  if (out == nullptr) {
    return false;
  }
  std::string token = trim(type_token);
  std::string token_upper = to_upper_copy(token);
  if (token_upper.rfind("OFGDB_GEOMETRY(", 0) != 0 || token.empty() || token.back() != ')') {
    return false;
  }
  size_t open = token.find('(');
  size_t close = token.rfind(')');
  if (open == std::string::npos || close == std::string::npos || close <= open + 1) {
    return false;
  }
  std::vector<std::string> args = split(token.substr(open + 1, close - open - 1), ',');
  if (args.size() != 3) {
    return false;
  }
  int epsg = 0;
  int dim = 0;
  if (!parse_int32_strict(args[1], &epsg) || !parse_int32_strict(args[2], &dim)) {
    return false;
  }
  GeometryContractKind contract_kind = GeometryContractKind::kUnknown;
  OGRwkbGeometryType type = map_geometry_kind(args[0], dim, &contract_kind);
  if (type == wkbUnknown) {
    return false;
  }

  out->name = column_name;
  out->contract_kind = contract_kind;
  out->ogr_type = type;
  out->epsg = epsg;
  out->nullable = !contains_ci(full_definition, "NOT NULL");
  return true;
}

bool contains_int64_bound_hint(const std::string& column_definition) {
  if (column_definition.empty()) {
    return false;
  }
  for (size_t i = 0; i < column_definition.size();) {
    bool negative = false;
    if (column_definition[i] == '-' && i + 1 < column_definition.size() &&
        std::isdigit(static_cast<unsigned char>(column_definition[i + 1])) != 0) {
      negative = true;
      i++;
    }
    if (std::isdigit(static_cast<unsigned char>(column_definition[i])) == 0) {
      i++;
      continue;
    }
    size_t start = i;
    while (i < column_definition.size() && std::isdigit(static_cast<unsigned char>(column_definition[i])) != 0) {
      i++;
    }
    std::string token = column_definition.substr(start, i - start);
    try {
      long long value = std::stoll(token);
      if ((!negative && value > static_cast<long long>(std::numeric_limits<int32_t>::max())) ||
          (negative && (-value) < static_cast<long long>(std::numeric_limits<int32_t>::min()))) {
        return true;
      }
    } catch (const std::exception&) {
      // Ignore unparseable fragments and continue scanning.
    }
  }
  return false;
}

OGRFieldType map_field_type_from_column_definition(const std::string& type_name, const std::string& full_definition) {
  OGRFieldType type = map_field_type_from_string(type_name);
  if (type == OFTInteger && contains_int64_bound_hint(full_definition)) {
    return OFTInteger64;
  }
  return type;
}

void set_field_from_literal(OGRFeatureH feat, OGRFieldDefnH fld_defn, int idx, const std::string& raw_value) {
  std::string value = trim(raw_value);
  if (equals_ci(value, "NULL")) {
    OGR_F_SetFieldNull(feat, idx);
    return;
  }
  OGRFieldType type = OGR_Fld_GetType(fld_defn);
  if (!value.empty() && value.front() == '\'') {
    value = unquote(value);
  }
  switch (type) {
    case OFTInteger:
      OGR_F_SetFieldInteger(feat, idx, std::atoi(value.c_str()));
      return;
    case OFTInteger64:
      OGR_F_SetFieldInteger64(feat, idx, static_cast<GIntBig>(std::atoll(value.c_str())));
      return;
    case OFTReal:
      OGR_F_SetFieldDouble(feat, idx, std::atof(value.c_str()));
      return;
    case OFTBinary:
      {
        std::vector<GByte> decoded;
        if (decode_byte_literal_value(value, &decoded)) {
          OGR_F_SetFieldBinary(feat, idx, static_cast<int>(decoded.size()), decoded.empty() ? nullptr : decoded.data());
        } else {
          OGR_F_SetFieldBinary(feat, idx, static_cast<int>(value.size()), reinterpret_cast<const GByte*>(value.data()));
        }
      }
      return;
    default:
      OGR_F_SetFieldString(feat, idx, value.c_str());
      return;
  }
}

std::string format_filter_literal(OGRFeatureH feat, int field_idx) {
  OGRFieldDefnH fld_defn = OGR_F_GetFieldDefnRef(feat, field_idx);
  if (fld_defn == nullptr) {
    return "";
  }
  OGRFieldType type = OGR_Fld_GetType(fld_defn);
  if (type == OFTInteger || type == OFTInteger64) {
    return std::to_string(OGR_F_GetFieldAsInteger64(feat, field_idx));
  }
  if (type == OFTReal) {
    return std::to_string(OGR_F_GetFieldAsDouble(feat, field_idx));
  }
  std::string value = OGR_F_GetFieldAsString(feat, field_idx);
  std::string escaped;
  escaped.reserve(value.size() + 8);
  escaped.push_back('\'');
  for (char c : value) {
    if (c == '\'') {
      escaped += "''";
    } else {
      escaped.push_back(c);
    }
  }
  escaped.push_back('\'');
  return escaped;
}

GDALRelationshipCardinality parse_cardinality(const char* cardinality_raw) {
  std::string cardinality = cardinality_raw != nullptr ? to_lower_copy(trim(cardinality_raw)) : "";
  if (cardinality == "1:1" || cardinality == "one_to_one" || cardinality == "one-to-one") {
    return GRC_ONE_TO_ONE;
  }
  if (cardinality == "1:n" || cardinality == "one_to_many" || cardinality == "one-to-many") {
    return GRC_ONE_TO_MANY;
  }
  if (cardinality == "n:1" || cardinality == "many_to_one" || cardinality == "many-to-one") {
    return GRC_MANY_TO_ONE;
  }
  if (cardinality == "m:n" || cardinality == "n:n" || cardinality == "many_to_many" || cardinality == "many-to-many") {
    return GRC_MANY_TO_MANY;
  }
  return GRC_ONE_TO_MANY;
}

void destroy_coded_values(std::vector<OGRCodedValue>& coded_values) {
  for (OGRCodedValue& value : coded_values) {
    CPLFree(value.pszCode);
    CPLFree(value.pszValue);
    value.pszCode = nullptr;
    value.pszValue = nullptr;
  }
}

}  // namespace

namespace openfgdb {

class GdalBackend final : public OpenFgdbBackend {
 public:
  GdalBackend() {
    static std::once_flag once;
    std::call_once(once, []() {
      GDALAllRegister();
    });
  }

  const char* backend_name() const override {
    return "gdal";
  }

  std::string runtime_info() const override {
    return std::string("backend=gdal;impl=real_gdal;gdal_tag=v3.12.0;mode=strict_explicit;lib=") + gdal_runtime_library_hint();
  }

  const char* last_error_message() const override {
    return last_error_.c_str();
  }

  int open(const char* path, uint64_t* db_handle) override {
    if (should_force_fail()) {
      return fail(OFGDB_ERR_INTERNAL, "forced gdal backend failure via OPENFGDB4J_GDAL_FORCE_FAIL");
    }
    if (path == nullptr || *path == '\0' || db_handle == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "path/output handle missing");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    CPLErrorReset();
    char** open_options = nullptr;
    open_options = CSLAddString(open_options, "LIST_ALL_TABLES=YES");
    GDALDatasetH ds = GDALOpenEx(path, GDAL_OF_VECTOR | GDAL_OF_UPDATE, nullptr, open_options, nullptr);
    CSLDestroy(open_options);
    if (ds == nullptr) {
      return fail_from_cpl(OFGDB_ERR_NOT_FOUND, "failed to open FileGDB");
    }
    uint64_t handle = allocate_handle_locked();
    dbs_[handle] = DbState{ds, path};
    std::string load_error;
    if (!load_geometry_contracts_locked(dbs_[handle], &load_error)) {
      GDALClose(ds);
      dbs_.erase(handle);
      return fail(OFGDB_ERR_INTERNAL, load_error);
    }
    *db_handle = handle;
    last_error_.clear();
    return OFGDB_OK;
  }

  int create(const char* path, uint64_t* db_handle) override {
    if (should_force_fail()) {
      return fail(OFGDB_ERR_INTERNAL, "forced gdal backend failure via OPENFGDB4J_GDAL_FORCE_FAIL");
    }
    if (path == nullptr || *path == '\0' || db_handle == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "path/output handle missing");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    CPLErrorReset();
    GDALDriverH drv = GDALGetDriverByName("OpenFileGDB");
    if (drv == nullptr) {
      return fail(OFGDB_ERR_INTERNAL, "OpenFileGDB driver not available in GDAL runtime");
    }
    GDALDatasetH ds = GDALCreate(drv, path, 0, 0, 0, GDT_Unknown, nullptr);
    if (ds == nullptr) {
      return fail_from_cpl(OFGDB_ERR_INTERNAL, "failed to create FileGDB");
    }
    uint64_t handle = allocate_handle_locked();
    dbs_[handle] = DbState{ds, path};
    std::string load_error;
    if (!load_geometry_contracts_locked(dbs_[handle], &load_error)) {
      GDALClose(ds);
      dbs_.erase(handle);
      return fail(OFGDB_ERR_INTERNAL, load_error);
    }
    *db_handle = handle;
    last_error_.clear();
    return OFGDB_OK;
  }

  int close(uint64_t db_handle) override {
    std::lock_guard<std::mutex> lock(mutex_);
    auto db_it = dbs_.find(db_handle);
    if (db_it == dbs_.end()) {
      return OFGDB_OK;
    }
    for (auto it = rows_.begin(); it != rows_.end();) {
      if (it->second.db_handle == db_handle) {
        destroy_row_locked(it->second);
        it = rows_.erase(it);
      } else {
        ++it;
      }
    }
    for (auto it = cursors_.begin(); it != cursors_.end();) {
      if (it->second.db_handle == db_handle) {
        destroy_cursor_locked(it->second);
        it = cursors_.erase(it);
      } else {
        ++it;
      }
    }
    for (auto it = tables_.begin(); it != tables_.end();) {
      if (it->second.db_handle == db_handle) {
        it = tables_.erase(it);
      } else {
        ++it;
      }
    }
    for (auto it = field_infos_.begin(); it != field_infos_.end();) {
      if (it->second.db_handle == db_handle) {
        it = field_infos_.erase(it);
      } else {
        ++it;
      }
    }
    GDALClose(db_it->second.dataset);
    dbs_.erase(db_it);
    last_error_.clear();
    return OFGDB_OK;
  }

  int exec_sql(uint64_t db_handle, const char* sql) override {
    if (sql == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "sql is null");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    DbState* db = get_db_locked(db_handle);
    if (db == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
    }
    std::string stmt = trim(sql);
    if (starts_with_ci(stmt, "CREATE TABLE") ||
        starts_with_ci(stmt, "INSERT INTO") ||
        starts_with_ci(stmt, "UPDATE") ||
        starts_with_ci(stmt, "DELETE FROM") ||
        starts_with_ci(stmt, "DROP TABLE")) {
      return exec_sql_fallback_locked(*db, sql, db_handle);
    }
    int native_rc = exec_sql_native_locked(*db, sql);
    if (native_rc == OFGDB_OK) {
      last_error_.clear();
      return OFGDB_OK;
    }
    return exec_sql_fallback_locked(*db, sql, db_handle);
  }

  int open_table(uint64_t db_handle, const char* table_name, uint64_t* table_handle) override {
    if (table_name == nullptr || *table_name == '\0' || table_handle == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "table name/output handle missing");
    }
    if (is_internal_table_name(table_name)) {
      return fail(OFGDB_ERR_NOT_FOUND, std::string("table not found: ") + table_name);
    }
    std::lock_guard<std::mutex> lock(mutex_);
    DbState* db = get_db_locked(db_handle);
    if (db == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
    }
    OGRLayerH layer = GDALDatasetGetLayerByName(db->dataset, table_name);
    if (layer == nullptr) {
      return fail(OFGDB_ERR_NOT_FOUND, std::string("table not found: ") + table_name);
    }
    uint64_t handle = allocate_handle_locked();
    tables_[handle] = TableState{db_handle, OGR_L_GetName(layer)};
    *table_handle = handle;
    last_error_.clear();
    return OFGDB_OK;
  }

  int close_table(uint64_t db_handle, uint64_t table_handle) override {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = tables_.find(table_handle);
    if (it == tables_.end()) {
      return OFGDB_OK;
    }
    if (it->second.db_handle != db_handle) {
      return fail(OFGDB_ERR_INVALID_ARG, "table handle does not belong to db");
    }
    tables_.erase(it);
    last_error_.clear();
    return OFGDB_OK;
  }

  int search(uint64_t table_handle, const char* fields, const char* where_clause, uint64_t* cursor_handle) override {
    if (cursor_handle == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "output cursor handle missing");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    TableState* table = get_table_locked(table_handle);
    if (table == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown table handle");
    }
    DbState* db = get_db_locked(table->db_handle);
    if (db == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
    }

    std::string select_fields = fields != nullptr && *fields != '\0' ? fields : "*";
    std::string sql = "SELECT " + select_fields + " FROM " + quote_identifier(table->layer_name);
    std::string where = where_clause != nullptr ? trim(where_clause) : "";
    if (!where.empty()) {
      sql += " WHERE " + where;
    }

    CPLErrorReset();
    OGRLayerH result_layer = GDALDatasetExecuteSQL(db->dataset, sql.c_str(), nullptr, nullptr);
    if (result_layer != nullptr) {
      uint64_t handle = allocate_handle_locked();
      CursorState state;
      state.db_handle = table->db_handle;
      state.layer_name = table->layer_name;
      state.kind = CursorKind::kLayerResultSet;
      state.layer = result_layer;
      state.release_result_set = true;
      cursors_[handle] = std::move(state);
      *cursor_handle = handle;
      last_error_.clear();
      return OFGDB_OK;
    }

    OGRLayerH layer = GDALDatasetGetLayerByName(db->dataset, table->layer_name.c_str());
    if (layer == nullptr) {
      return fail(OFGDB_ERR_NOT_FOUND, std::string("table not found: ") + table->layer_name);
    }
    OGRErr err = OGR_L_SetAttributeFilter(layer, where.empty() ? nullptr : where.c_str());
    if (err != OGRERR_NONE) {
      return fail_from_cpl(OFGDB_ERR_INVALID_ARG, "failed to set attribute filter");
    }
    OGR_L_ResetReading(layer);

    uint64_t handle = allocate_handle_locked();
    CursorState state;
    state.db_handle = table->db_handle;
    state.layer_name = table->layer_name;
    state.kind = CursorKind::kLayer;
    state.layer = layer;
    state.release_result_set = false;
    cursors_[handle] = std::move(state);
    *cursor_handle = handle;
    last_error_.clear();
    return OFGDB_OK;
  }

  int fetch_row(uint64_t cursor_handle, uint64_t* row_handle) override {
    if (row_handle == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "output row handle missing");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    CursorState* cursor = get_cursor_locked(cursor_handle);
    if (cursor == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown cursor handle");
    }

    if (cursor->kind == CursorKind::kSyntheticRows) {
      if (cursor->synthetic_index >= cursor->synthetic_rows.size()) {
        *row_handle = 0;
        return OFGDB_OK;
      }
      RowState row;
      row.db_handle = cursor->db_handle;
      row.layer_name = cursor->layer_name;
      row.kind = RowKind::kSynthetic;
      row.synthetic_values = cursor->synthetic_rows[cursor->synthetic_index++];
      uint64_t handle = allocate_handle_locked();
      rows_[handle] = std::move(row);
      *row_handle = handle;
      last_error_.clear();
      return OFGDB_OK;
    }

    if (cursor->layer == nullptr) {
      return fail(OFGDB_ERR_INTERNAL, "cursor has no layer");
    }
    OGRFeatureH feat = OGR_L_GetNextFeature(cursor->layer);
    if (feat == nullptr) {
      *row_handle = 0;
      return OFGDB_OK;
    }
    RowState row;
    row.db_handle = cursor->db_handle;
    if (!cursor->layer_name.empty()) {
      row.layer_name = cursor->layer_name;
    } else {
      const char* layer_name = OGR_L_GetName(cursor->layer);
      if (layer_name != nullptr) {
        row.layer_name = layer_name;
      }
    }
    row.kind = RowKind::kFeature;
    row.feature = feat;
    row.owns_feature = true;
    uint64_t handle = allocate_handle_locked();
    rows_[handle] = std::move(row);
    *row_handle = handle;
    last_error_.clear();
    return OFGDB_OK;
  }

  int close_cursor(uint64_t cursor_handle) override {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = cursors_.find(cursor_handle);
    if (it == cursors_.end()) {
      return OFGDB_OK;
    }
    destroy_cursor_locked(it->second);
    cursors_.erase(it);
    last_error_.clear();
    return OFGDB_OK;
  }

  int create_row(uint64_t table_handle, uint64_t* row_handle) override {
    if (row_handle == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "output row handle missing");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    TableState* table = get_table_locked(table_handle);
    if (table == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown table handle");
    }
    DbState* db = get_db_locked(table->db_handle);
    if (db == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
    }
    OGRLayerH layer = GDALDatasetGetLayerByName(db->dataset, table->layer_name.c_str());
    if (layer == nullptr) {
      return fail(OFGDB_ERR_NOT_FOUND, std::string("table not found: ") + table->layer_name);
    }
    OGRFeatureDefnH defn = OGR_L_GetLayerDefn(layer);
    if (defn == nullptr) {
      return fail(OFGDB_ERR_INTERNAL, "failed to get layer definition");
    }
    OGRFeatureH feat = OGR_F_Create(defn);
    if (feat == nullptr) {
      return fail(OFGDB_ERR_INTERNAL, "failed to create feature");
    }
    RowState row;
    row.db_handle = table->db_handle;
    row.layer_name = table->layer_name;
    row.kind = RowKind::kFeature;
    row.feature = feat;
    row.owns_feature = true;
    uint64_t handle = allocate_handle_locked();
    rows_[handle] = std::move(row);
    *row_handle = handle;
    last_error_.clear();
    return OFGDB_OK;
  }

  int insert(uint64_t table_handle, uint64_t row_handle) override {
    std::lock_guard<std::mutex> lock(mutex_);
    TableState* table = get_table_locked(table_handle);
    RowState* row = get_row_locked(row_handle);
    if (table == nullptr || row == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown table or row handle");
    }
    if (table->db_handle != row->db_handle) {
      return fail(OFGDB_ERR_INVALID_ARG, "table and row belong to different databases");
    }
    if (row->kind != RowKind::kFeature || row->feature == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "row handle is not feature-backed");
    }
    DbState* db = get_db_locked(table->db_handle);
    if (db == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
    }
    OGRLayerH layer = GDALDatasetGetLayerByName(db->dataset, table->layer_name.c_str());
    if (layer == nullptr) {
      return fail(OFGDB_ERR_NOT_FOUND, std::string("table not found: ") + table->layer_name);
    }
    OGRErr err = OGR_L_CreateFeature(layer, row->feature);
    if (err != OGRERR_NONE) {
      return fail_from_cpl(map_ogr_error(err), "failed to insert feature");
    }
    last_error_.clear();
    return OFGDB_OK;
  }

  int update(uint64_t table_handle, uint64_t row_handle) override {
    std::lock_guard<std::mutex> lock(mutex_);
    TableState* table = get_table_locked(table_handle);
    RowState* row = get_row_locked(row_handle);
    if (table == nullptr || row == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown table or row handle");
    }
    if (table->db_handle != row->db_handle) {
      return fail(OFGDB_ERR_INVALID_ARG, "table and row belong to different databases");
    }
    if (row->kind != RowKind::kFeature || row->feature == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "row handle is not feature-backed");
    }
    DbState* db = get_db_locked(table->db_handle);
    if (db == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
    }
    OGRLayerH layer = GDALDatasetGetLayerByName(db->dataset, table->layer_name.c_str());
    if (layer == nullptr) {
      return fail(OFGDB_ERR_NOT_FOUND, std::string("table not found: ") + table->layer_name);
    }

    if (OGR_F_GetFID(row->feature) < 0) {
      const char* key_candidates[] = {"T_Id", "T_ID", "OBJECTID"};
      for (const char* key : key_candidates) {
        int idx = find_field_index_ci(OGR_F_GetDefnRef(row->feature), key);
        if (idx < 0 || !OGR_F_IsFieldSetAndNotNull(row->feature, idx)) {
          continue;
        }
        std::string filter = quote_identifier(key) + " = " + format_filter_literal(row->feature, idx);
        OGR_L_SetAttributeFilter(layer, filter.c_str());
        OGR_L_ResetReading(layer);
        OGRFeatureH current = OGR_L_GetNextFeature(layer);
        OGR_L_SetAttributeFilter(layer, nullptr);
        if (current != nullptr) {
          OGR_F_SetFID(row->feature, OGR_F_GetFID(current));
          OGR_F_Destroy(current);
          break;
        }
      }
    }

    OGRErr err = OGRERR_NONE;
    if (OGR_F_GetFID(row->feature) >= 0) {
      err = OGR_L_SetFeature(layer, row->feature);
    } else {
      err = OGR_L_CreateFeature(layer, row->feature);
    }
    if (err != OGRERR_NONE) {
      return fail_from_cpl(map_ogr_error(err), "failed to update feature");
    }
    last_error_.clear();
    return OFGDB_OK;
  }

  int close_row(uint64_t row_handle) override {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = rows_.find(row_handle);
    if (it == rows_.end()) {
      return OFGDB_OK;
    }
    destroy_row_locked(it->second);
    rows_.erase(it);
    last_error_.clear();
    return OFGDB_OK;
  }

  int get_field_info(uint64_t table_handle, uint64_t* field_info_handle) override {
    if (field_info_handle == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "output field info handle missing");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    TableState* table = get_table_locked(table_handle);
    if (table == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown table handle");
    }
    DbState* db = get_db_locked(table->db_handle);
    if (db == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
    }
    OGRLayerH layer = GDALDatasetGetLayerByName(db->dataset, table->layer_name.c_str());
    if (layer == nullptr) {
      return fail(OFGDB_ERR_NOT_FOUND, std::string("table not found: ") + table->layer_name);
    }
    OGRFeatureDefnH defn = OGR_L_GetLayerDefn(layer);
    if (defn == nullptr) {
      return fail(OFGDB_ERR_INTERNAL, "failed to get layer definition");
    }
    FieldInfoState info;
    info.db_handle = table->db_handle;
    int field_count = OGR_FD_GetFieldCount(defn);
    for (int i = 0; i < field_count; i++) {
      OGRFieldDefnH fld = OGR_FD_GetFieldDefn(defn, i);
      if (fld != nullptr) {
        info.names.emplace_back(OGR_Fld_GetNameRef(fld));
      }
    }
    int geom_count = OGR_FD_GetGeomFieldCount(defn);
    for (int i = 0; i < geom_count; i++) {
      OGRGeomFieldDefnH geom = OGR_FD_GetGeomFieldDefn(defn, i);
      if (geom != nullptr) {
        info.names.emplace_back(OGR_GFld_GetNameRef(geom));
      }
    }
    uint64_t handle = allocate_handle_locked();
    field_infos_[handle] = std::move(info);
    *field_info_handle = handle;
    last_error_.clear();
    return OFGDB_OK;
  }

  int close_field_info(uint64_t field_info_handle) override {
    std::lock_guard<std::mutex> lock(mutex_);
    field_infos_.erase(field_info_handle);
    last_error_.clear();
    return OFGDB_OK;
  }

  int field_info_count(uint64_t field_info_handle, int32_t* out_count) override {
    if (out_count == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "output count missing");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = field_infos_.find(field_info_handle);
    if (it == field_infos_.end()) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown field info handle");
    }
    if (it->second.names.size() > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
      return fail(OFGDB_ERR_INTERNAL, "field count too large");
    }
    *out_count = static_cast<int32_t>(it->second.names.size());
    last_error_.clear();
    return OFGDB_OK;
  }

  int field_info_name(uint64_t field_info_handle, int32_t index, char** out_name) override {
    if (out_name == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "output name missing");
    }
    *out_name = nullptr;
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = field_infos_.find(field_info_handle);
    if (it == field_infos_.end()) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown field info handle");
    }
    if (index < 0 || static_cast<size_t>(index) >= it->second.names.size()) {
      return fail(OFGDB_ERR_INVALID_ARG, "field info index out of range");
    }
    char* duplicated = dup_cstr(it->second.names[static_cast<size_t>(index)]);
    if (duplicated == nullptr) {
      return fail(OFGDB_ERR_INTERNAL, "out of memory");
    }
    *out_name = duplicated;
    last_error_.clear();
    return OFGDB_OK;
  }

  int set_string(uint64_t row_handle, const char* column_name, const char* value) override {
    std::lock_guard<std::mutex> lock(mutex_);
    RowState* row = get_row_locked(row_handle);
    if (row == nullptr || row->kind != RowKind::kFeature || row->feature == nullptr || column_name == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "invalid row handle or column name");
    }
    int idx = find_field_index_ci(OGR_F_GetDefnRef(row->feature), column_name);
    if (idx < 0) {
      return fail(OFGDB_ERR_NOT_FOUND, std::string("unknown column: ") + column_name);
    }
    OGR_F_SetFieldString(row->feature, idx, value != nullptr ? value : "");
    last_error_.clear();
    return OFGDB_OK;
  }

  int set_int32(uint64_t row_handle, const char* column_name, int32_t value) override {
    std::lock_guard<std::mutex> lock(mutex_);
    RowState* row = get_row_locked(row_handle);
    if (row == nullptr || row->kind != RowKind::kFeature || row->feature == nullptr || column_name == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "invalid row handle or column name");
    }
    int idx = find_field_index_ci(OGR_F_GetDefnRef(row->feature), column_name);
    if (idx < 0) {
      return fail(OFGDB_ERR_NOT_FOUND, std::string("unknown column: ") + column_name);
    }
    OGR_F_SetFieldInteger(row->feature, idx, value);
    last_error_.clear();
    return OFGDB_OK;
  }

  int set_double(uint64_t row_handle, const char* column_name, double value) override {
    std::lock_guard<std::mutex> lock(mutex_);
    RowState* row = get_row_locked(row_handle);
    if (row == nullptr || row->kind != RowKind::kFeature || row->feature == nullptr || column_name == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "invalid row handle or column name");
    }
    int idx = find_field_index_ci(OGR_F_GetDefnRef(row->feature), column_name);
    if (idx < 0) {
      return fail(OFGDB_ERR_NOT_FOUND, std::string("unknown column: ") + column_name);
    }
    OGR_F_SetFieldDouble(row->feature, idx, value);
    last_error_.clear();
    return OFGDB_OK;
  }

  int set_blob(uint64_t row_handle, const char* column_name, const uint8_t* data, int32_t size) override {
    if (size < 0) {
      return fail(OFGDB_ERR_INVALID_ARG, "blob size must be >= 0");
    }
    if (data == nullptr && size > 0) {
      return fail(OFGDB_ERR_INVALID_ARG, "blob payload missing");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    RowState* row = get_row_locked(row_handle);
    if (row == nullptr || row->kind != RowKind::kFeature || row->feature == nullptr || column_name == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "invalid row handle or column name");
    }
    int idx = find_field_index_ci(OGR_F_GetDefnRef(row->feature), column_name);
    if (idx >= 0) {
      OGR_F_SetFieldBinary(row->feature, idx, size, data);
      last_error_.clear();
      return OFGDB_OK;
    }
    int geom_idx = resolve_geom_field_index_for_row_locked(row, column_name);
    if (geom_idx >= 0) {
      if (size == 0) {
        OGRErr err = OGR_F_SetGeomField(row->feature, geom_idx, nullptr);
        if (err != OGRERR_NONE) {
          return fail_from_cpl(map_ogr_error(err), "failed to clear geometry");
        }
        last_error_.clear();
        return OFGDB_OK;
      }
      OGRGeometryH geom = nullptr;
      OGRErr err = OGR_G_CreateFromWkb(data, nullptr, &geom, size);
      if (err != OGRERR_NONE || geom == nullptr) {
        return fail_from_cpl(OFGDB_ERR_INVALID_ARG, "invalid WKB geometry");
      }
      std::string geometry_error;
      if (!check_geometry_contract_for_row_locked(row, geom_idx, geom, &geometry_error)) {
        OGR_G_DestroyGeometry(geom);
        return fail(OFGDB_ERR_INVALID_ARG, geometry_error);
      }
      err = OGR_F_SetGeomFieldDirectly(row->feature, geom_idx, geom);
      if (err != OGRERR_NONE) {
        OGR_G_DestroyGeometry(geom);
        return fail_from_cpl(map_ogr_error(err), "failed to set geometry");
      }
      last_error_.clear();
      return OFGDB_OK;
    }
    return fail(OFGDB_ERR_NOT_FOUND, std::string("unknown column: ") + column_name);
  }

  int set_geometry(uint64_t row_handle, const uint8_t* wkb, int32_t size) override {
    if (size < 0) {
      return fail(OFGDB_ERR_INVALID_ARG, "geometry size must be >= 0");
    }
    if (wkb == nullptr && size > 0) {
      return fail(OFGDB_ERR_INVALID_ARG, "geometry payload missing");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    RowState* row = get_row_locked(row_handle);
    if (row == nullptr || row->kind != RowKind::kFeature || row->feature == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "invalid row handle");
    }
    if (OGR_F_GetGeomFieldCount(row->feature) <= 0) {
      return fail(OFGDB_ERR_NOT_FOUND, "feature has no geometry field");
    }
    if (wkb == nullptr || size == 0) {
      OGRErr err = OGR_F_SetGeomField(row->feature, 0, nullptr);
      if (err != OGRERR_NONE) {
        return fail_from_cpl(map_ogr_error(err), "failed to clear geometry");
      }
      return OFGDB_OK;
    }
    OGRGeometryH geom = nullptr;
    OGRErr err = OGR_G_CreateFromWkb(wkb, nullptr, &geom, size);
    if (err != OGRERR_NONE || geom == nullptr) {
      return fail_from_cpl(OFGDB_ERR_INVALID_ARG, "invalid WKB geometry");
    }
    std::string geometry_error;
    if (!check_geometry_contract_for_row_locked(row, 0, geom, &geometry_error)) {
      OGR_G_DestroyGeometry(geom);
      return fail(OFGDB_ERR_INVALID_ARG, geometry_error);
    }
    err = OGR_F_SetGeomFieldDirectly(row->feature, 0, geom);
    if (err != OGRERR_NONE) {
      OGR_G_DestroyGeometry(geom);
      return fail_from_cpl(map_ogr_error(err), "failed to set geometry");
    }
    last_error_.clear();
    return OFGDB_OK;
  }

  int set_null(uint64_t row_handle, const char* column_name) override {
    std::lock_guard<std::mutex> lock(mutex_);
    RowState* row = get_row_locked(row_handle);
    if (row == nullptr || row->kind != RowKind::kFeature || row->feature == nullptr || column_name == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "invalid row handle or column name");
    }
    int idx = find_field_index_ci(OGR_F_GetDefnRef(row->feature), column_name);
    if (idx >= 0) {
      OGR_F_SetFieldNull(row->feature, idx);
      last_error_.clear();
      return OFGDB_OK;
    }
    int geom_idx = resolve_geom_field_index_for_row_locked(row, column_name);
    if (geom_idx >= 0) {
      OGRErr err = OGR_F_SetGeomField(row->feature, geom_idx, nullptr);
      if (err != OGRERR_NONE) {
        return fail_from_cpl(map_ogr_error(err), "failed to clear geometry");
      }
      last_error_.clear();
      return OFGDB_OK;
    }
    return fail(OFGDB_ERR_NOT_FOUND, std::string("unknown column: ") + column_name);
  }

  int list_domains(uint64_t db_handle, uint64_t* cursor_handle) override {
    if (cursor_handle == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "output cursor handle missing");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    DbState* db = get_db_locked(db_handle);
    if (db == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
    }
    char** names = GDALDatasetGetFieldDomainNames(db->dataset, nullptr);
    CursorState cursor;
    cursor.db_handle = db_handle;
    cursor.kind = CursorKind::kSyntheticRows;
    if (names != nullptr) {
      int count = CSLCount(names);
      for (int i = 0; i < count; i++) {
        const char* name = names[i];
        if (name == nullptr) {
          continue;
        }
        std::unordered_map<std::string, SyntheticValue> row;
        row["name"] = SyntheticValue::from_string(name);
        OGRFieldDomainH domain = GDALDatasetGetFieldDomain(db->dataset, name);
        if (domain != nullptr) {
          row["fieldType"] = SyntheticValue::from_string(map_field_type_to_symbolic_name(OGR_FldDomain_GetFieldType(domain)));
        } else {
          row["fieldType"] = SyntheticValue::from_string("STRING");
        }
        cursor.synthetic_rows.push_back(std::move(row));
      }
      CSLDestroy(names);
    }
    uint64_t handle = allocate_handle_locked();
    cursors_[handle] = std::move(cursor);
    *cursor_handle = handle;
    last_error_.clear();
    return OFGDB_OK;
  }

  int create_coded_domain(uint64_t db_handle, const char* domain_name, const char* field_type) override {
    if (domain_name == nullptr || *domain_name == '\0') {
      return fail(OFGDB_ERR_INVALID_ARG, "domain name missing");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    DbState* db = get_db_locked(db_handle);
    if (db == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
    }
    OGRFieldDomainH existing = GDALDatasetGetFieldDomain(db->dataset, domain_name);
    if (existing != nullptr) {
      last_error_.clear();
      return OFGDB_OK;
    }
    OGRCodedValue coded_values[1] = {{nullptr, nullptr}};
    OGRFieldDomainH domain =
        OGR_CodedFldDomain_Create(domain_name, "", map_field_type_from_symbolic_name(field_type != nullptr ? field_type : "STRING"), OFSTNone,
                                  coded_values);
    if (domain == nullptr) {
      return fail(OFGDB_ERR_INTERNAL, "failed to allocate coded domain");
    }
    char* failure_reason = nullptr;
    bool ok = GDALDatasetAddFieldDomain(db->dataset, domain, &failure_reason);
    OGR_FldDomain_Destroy(domain);
    if (!ok) {
      std::string reason = failure_reason != nullptr ? failure_reason : "";
      CPLFree(failure_reason);
      if (contains_ci(reason, "already")) {
        last_error_.clear();
        return OFGDB_OK;
      }
      return fail(OFGDB_ERR_INTERNAL, std::string("failed to create coded domain: ") + reason);
    }
    CPLFree(failure_reason);
    last_error_.clear();
    return OFGDB_OK;
  }

  int add_coded_value(uint64_t db_handle, const char* domain_name, const char* code, const char* label) override {
    if (domain_name == nullptr || code == nullptr || *domain_name == '\0' || *code == '\0') {
      return fail(OFGDB_ERR_INVALID_ARG, "domain/code missing");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    DbState* db = get_db_locked(db_handle);
    if (db == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
    }
    OGRFieldDomainH existing = GDALDatasetGetFieldDomain(db->dataset, domain_name);
    if (existing == nullptr) {
      return fail(OFGDB_ERR_NOT_FOUND, std::string("domain not found: ") + domain_name);
    }
    if (OGR_FldDomain_GetDomainType(existing) != OFDT_CODED) {
      return fail(OFGDB_ERR_INVALID_ARG, std::string("domain is not coded: ") + domain_name);
    }

    std::vector<std::pair<std::string, std::string>> values;
    const OGRCodedValue* enum_values = OGR_CodedFldDomain_GetEnumeration(existing);
    if (enum_values != nullptr) {
      for (int i = 0; enum_values[i].pszCode != nullptr; i++) {
        values.emplace_back(enum_values[i].pszCode, enum_values[i].pszValue != nullptr ? enum_values[i].pszValue : "");
      }
    }
    bool updated = false;
    for (std::pair<std::string, std::string>& entry : values) {
      if (equals_ci(entry.first, code)) {
        std::string wanted = label != nullptr ? label : code;
        if (entry.second == wanted) {
          last_error_.clear();
          return OFGDB_OK;
        }
        entry.second = wanted;
        updated = true;
        break;
      }
    }
    if (!updated) {
      values.emplace_back(code, label != nullptr ? label : code);
    }

    std::vector<OGRCodedValue> coded_values;
    coded_values.reserve(values.size() + 1);
    for (const std::pair<std::string, std::string>& value : values) {
      OGRCodedValue coded_value;
      coded_value.pszCode = CPLStrdup(value.first.c_str());
      coded_value.pszValue = CPLStrdup(value.second.c_str());
      coded_values.push_back(coded_value);
    }
    coded_values.push_back(OGRCodedValue{nullptr, nullptr});

    OGRFieldDomainH domain = OGR_CodedFldDomain_Create(
        domain_name,
        OGR_FldDomain_GetDescription(existing),
        OGR_FldDomain_GetFieldType(existing),
        OGR_FldDomain_GetFieldSubType(existing),
        coded_values.data());
    if (domain == nullptr) {
      destroy_coded_values(coded_values);
      return fail(OFGDB_ERR_INTERNAL, "failed to allocate updated coded domain");
    }

    char* failure_reason = nullptr;
    bool ok = GDALDatasetUpdateFieldDomain(db->dataset, domain, &failure_reason);
    OGR_FldDomain_Destroy(domain);
    destroy_coded_values(coded_values);
    if (!ok) {
      std::string reason = failure_reason != nullptr ? failure_reason : "";
      CPLFree(failure_reason);
      return fail(OFGDB_ERR_INTERNAL, std::string("failed to update coded domain: ") + reason);
    }
    CPLFree(failure_reason);
    last_error_.clear();
    return OFGDB_OK;
  }

  int assign_domain_to_field(uint64_t db_handle, const char* table_name, const char* column_name, const char* domain_name) override {
    if (table_name == nullptr || column_name == nullptr || domain_name == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "table/column/domain missing");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    DbState* db = get_db_locked(db_handle);
    if (db == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
    }
    if (GDALDatasetGetFieldDomain(db->dataset, domain_name) == nullptr) {
      return fail(OFGDB_ERR_NOT_FOUND, std::string("domain not found: ") + domain_name);
    }
    OGRLayerH layer = GDALDatasetGetLayerByName(db->dataset, table_name);
    if (layer == nullptr) {
      return fail(OFGDB_ERR_NOT_FOUND, std::string("table not found: ") + table_name);
    }
    OGRFeatureDefnH defn = OGR_L_GetLayerDefn(layer);
    if (defn == nullptr) {
      return fail(OFGDB_ERR_INTERNAL, "failed to get layer definition");
    }
    int field_idx = find_field_index_ci(defn, column_name);
    if (field_idx < 0) {
      return fail(OFGDB_ERR_NOT_FOUND, std::string("column not found: ") + column_name);
    }
    OGRFieldDefnH current_field = OGR_FD_GetFieldDefn(defn, field_idx);
    if (current_field == nullptr) {
      return fail(OFGDB_ERR_INTERNAL, "failed to get field definition");
    }
    const char* current_domain = OGR_Fld_GetDomainName(current_field);
    if (current_domain != nullptr && equals_ci(current_domain, domain_name)) {
      last_error_.clear();
      return OFGDB_OK;
    }
    OGRFieldDefnH new_field = OGR_Fld_Create(OGR_Fld_GetNameRef(current_field), OGR_Fld_GetType(current_field));
    if (new_field == nullptr) {
      return fail(OFGDB_ERR_INTERNAL, "failed to allocate field definition");
    }
    OGR_Fld_SetDomainName(new_field, domain_name);
    OGRErr err = OGR_L_AlterFieldDefn(layer, field_idx, new_field, ALTER_DOMAIN_FLAG);
    OGR_Fld_Destroy(new_field);
    if (err != OGRERR_NONE) {
      return fail_from_cpl(map_ogr_error(err), "failed to assign domain to field");
    }
    last_error_.clear();
    return OFGDB_OK;
  }

  int list_relationships(uint64_t db_handle, uint64_t* cursor_handle) override {
    if (cursor_handle == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "output cursor handle missing");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    DbState* db = get_db_locked(db_handle);
    if (db == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
    }
    char** names = GDALDatasetGetRelationshipNames(db->dataset, nullptr);
    CursorState cursor;
    cursor.db_handle = db_handle;
    cursor.kind = CursorKind::kSyntheticRows;
    if (names != nullptr) {
      int count = CSLCount(names);
      for (int i = 0; i < count; i++) {
        const char* name = names[i];
        if (name == nullptr) {
          continue;
        }
        std::unordered_map<std::string, SyntheticValue> row;
        row["name"] = SyntheticValue::from_string(name);
        cursor.synthetic_rows.push_back(std::move(row));
      }
      CSLDestroy(names);
    }
    uint64_t handle = allocate_handle_locked();
    cursors_[handle] = std::move(cursor);
    *cursor_handle = handle;
    last_error_.clear();
    return OFGDB_OK;
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
    if (name == nullptr || origin_table == nullptr || destination_table == nullptr || origin_pk == nullptr || origin_fk == nullptr ||
        cardinality == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "relationship input incomplete");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    DbState* db = get_db_locked(db_handle);
    if (db == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
    }
    GDALRelationshipH existing = GDALDatasetGetRelationship(db->dataset, name);
    if (existing != nullptr) {
      last_error_.clear();
      return OFGDB_OK;
    }
    if (relationship_signature_exists_locked(
            db->dataset,
            origin_table,
            destination_table,
            origin_pk,
            origin_fk,
            cardinality,
            is_composite != 0,
            is_attributed != 0)) {
      last_error_.clear();
      return OFGDB_OK;
    }

    GDALRelationshipCardinality relationship_cardinality = parse_cardinality(cardinality);
    GDALRelationshipH relationship = GDALRelationshipCreate(name, origin_table, destination_table, relationship_cardinality);
    if (relationship == nullptr) {
      return fail(OFGDB_ERR_INTERNAL, "failed to create relationship object");
    }

    char** left_fields = nullptr;
    char** right_fields = nullptr;
    left_fields = CSLAddString(left_fields, origin_pk);
    right_fields = CSLAddString(right_fields, origin_fk);
    GDALRelationshipSetLeftTableFields(relationship, left_fields);
    GDALRelationshipSetRightTableFields(relationship, right_fields);
    CSLDestroy(left_fields);
    CSLDestroy(right_fields);

    GDALRelationshipSetType(relationship, is_composite != 0 ? GRT_COMPOSITE : GRT_ASSOCIATION);
    if (forward_label != nullptr) {
      GDALRelationshipSetForwardPathLabel(relationship, forward_label);
    }
    if (backward_label != nullptr) {
      GDALRelationshipSetBackwardPathLabel(relationship, backward_label);
    }
    if (is_attributed != 0) {
      const bool many_to_many = (relationship_cardinality == GRC_MANY_TO_MANY);
      GDALRelationshipSetMappingTableName(relationship, many_to_many ? name : destination_table);
    }

    char* failure_reason = nullptr;
    bool ok = GDALDatasetAddRelationship(db->dataset, relationship, &failure_reason);
    GDALDestroyRelationship(relationship);
    if (!ok) {
      std::string reason = failure_reason != nullptr ? failure_reason : "";
      CPLFree(failure_reason);
      if (contains_ci(reason, "already")) {
        last_error_.clear();
        return OFGDB_OK;
      }
      return fail(OFGDB_ERR_INTERNAL, std::string("failed to create relationship class: ") + reason);
    }
    CPLFree(failure_reason);
    last_error_.clear();
    return OFGDB_OK;
  }

  int list_domains_text(uint64_t db_handle, char** out_text) override {
    return list_text_from_name_array(db_handle, out_text, true);
  }

  int list_relationships_text(uint64_t db_handle, char** out_text) override {
    return list_text_from_name_array(db_handle, out_text, false);
  }

  int list_tables_text(uint64_t db_handle, char** out_text) override {
    if (out_text == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "output pointer missing");
    }
    *out_text = nullptr;
    std::lock_guard<std::mutex> lock(mutex_);
    DbState* db = get_db_locked(db_handle);
    if (db == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
    }
    std::vector<std::string> names;
    int layer_count = GDALDatasetGetLayerCount(db->dataset);
    names.reserve(static_cast<size_t>(std::max(0, layer_count)));
    for (int i = 0; i < layer_count; i++) {
      OGRLayerH layer = GDALDatasetGetLayer(db->dataset, i);
      if (layer == nullptr) {
        continue;
      }
      const char* name = OGR_L_GetName(layer);
      if (name != nullptr) {
        if (is_internal_table_name(name)) {
          continue;
        }
        names.emplace_back(name);
      }
    }
    char* duplicated = dup_cstr(join_lines(names));
    if (duplicated == nullptr) {
      return fail(OFGDB_ERR_INTERNAL, "out of memory");
    }
    *out_text = duplicated;
    last_error_.clear();
    return OFGDB_OK;
  }

  int row_get_string(uint64_t row_handle, const char* column_name, char** out_value) override {
    if (column_name == nullptr || out_value == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "column/output missing");
    }
    *out_value = nullptr;
    std::lock_guard<std::mutex> lock(mutex_);
    RowState* row = get_row_locked(row_handle);
    if (row == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown row handle");
    }
    std::string value;
    bool is_set = false;
    if (row->kind == RowKind::kFeature && row->feature != nullptr) {
      int idx = find_field_index_ci(OGR_F_GetDefnRef(row->feature), column_name);
      if (idx >= 0 && OGR_F_IsFieldSetAndNotNull(row->feature, idx)) {
        value = OGR_F_GetFieldAsString(row->feature, idx);
        is_set = true;
      }
    } else {
      const SyntheticValue* v = get_synthetic_value_ci(row->synthetic_values, column_name);
      if (v != nullptr && v->type != SyntheticValue::Type::kNull) {
        value = v->string_value;
        is_set = true;
      }
    }
    if (!is_set) {
      last_error_.clear();
      return OFGDB_OK;
    }
    char* duplicated = dup_cstr(value);
    if (duplicated == nullptr) {
      return fail(OFGDB_ERR_INTERNAL, "out of memory");
    }
    *out_value = duplicated;
    last_error_.clear();
    return OFGDB_OK;
  }

  int row_is_null(uint64_t row_handle, const char* column_name, int32_t* out_is_null) override {
    if (column_name == nullptr || out_is_null == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "column/output missing");
    }
    *out_is_null = 1;
    std::lock_guard<std::mutex> lock(mutex_);
    RowState* row = get_row_locked(row_handle);
    if (row == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown row handle");
    }
    if (row->kind == RowKind::kFeature && row->feature != nullptr) {
      int idx = find_field_index_ci(OGR_F_GetDefnRef(row->feature), column_name);
      if (idx >= 0) {
        *out_is_null = OGR_F_IsFieldSetAndNotNull(row->feature, idx) ? 0 : 1;
      } else {
        int geom_idx = resolve_geom_field_index_for_row_locked(row, column_name);
        if (geom_idx < 0) {
          *out_is_null = 1;
        } else {
          *out_is_null = OGR_F_GetGeomFieldRef(row->feature, geom_idx) != nullptr ? 0 : 1;
        }
      }
    } else {
      const SyntheticValue* v = get_synthetic_value_ci(row->synthetic_values, column_name);
      *out_is_null = (v == nullptr || v->type == SyntheticValue::Type::kNull) ? 1 : 0;
    }
    last_error_.clear();
    return OFGDB_OK;
  }

  int row_get_int32(uint64_t row_handle, const char* column_name, int32_t* out_value) override {
    if (column_name == nullptr || out_value == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "column/output missing");
    }
    *out_value = 0;
    std::lock_guard<std::mutex> lock(mutex_);
    RowState* row = get_row_locked(row_handle);
    if (row == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown row handle");
    }
    if (row->kind == RowKind::kFeature && row->feature != nullptr) {
      int idx = find_field_index_ci(OGR_F_GetDefnRef(row->feature), column_name);
      if (idx >= 0 && OGR_F_IsFieldSetAndNotNull(row->feature, idx)) {
        OGRFieldDefnH fld = OGR_F_GetFieldDefnRef(row->feature, idx);
        if (fld == nullptr) {
          return fail(OFGDB_ERR_INTERNAL, "failed to inspect field definition");
        }
        OGRFieldType type = OGR_Fld_GetType(fld);
        if (type != OFTInteger && type != OFTInteger64) {
          return fail(OFGDB_ERR_INVALID_ARG, "row value is not int32");
        }
        GIntBig value = OGR_F_GetFieldAsInteger64(row->feature, idx);
        if (value > std::numeric_limits<int32_t>::max() || value < std::numeric_limits<int32_t>::min()) {
          return fail(OFGDB_ERR_INVALID_ARG, "int32 overflow");
        }
        *out_value = static_cast<int32_t>(value);
      }
      last_error_.clear();
      return OFGDB_OK;
    }
    const SyntheticValue* v = get_synthetic_value_ci(row->synthetic_values, column_name);
    if (v != nullptr && v->type == SyntheticValue::Type::kInt) {
      *out_value = v->int_value;
    }
    last_error_.clear();
    return OFGDB_OK;
  }

  int row_get_double(uint64_t row_handle, const char* column_name, double* out_value) override {
    if (column_name == nullptr || out_value == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "column/output missing");
    }
    *out_value = 0.0;
    std::lock_guard<std::mutex> lock(mutex_);
    RowState* row = get_row_locked(row_handle);
    if (row == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown row handle");
    }
    if (row->kind == RowKind::kFeature && row->feature != nullptr) {
      int idx = find_field_index_ci(OGR_F_GetDefnRef(row->feature), column_name);
      if (idx >= 0 && OGR_F_IsFieldSetAndNotNull(row->feature, idx)) {
        OGRFieldDefnH fld = OGR_F_GetFieldDefnRef(row->feature, idx);
        if (fld == nullptr) {
          return fail(OFGDB_ERR_INTERNAL, "failed to inspect field definition");
        }
        OGRFieldType type = OGR_Fld_GetType(fld);
        if (type != OFTReal) {
          return fail(OFGDB_ERR_INVALID_ARG, "row value is not double");
        }
        *out_value = OGR_F_GetFieldAsDouble(row->feature, idx);
      }
      last_error_.clear();
      return OFGDB_OK;
    }
    const SyntheticValue* v = get_synthetic_value_ci(row->synthetic_values, column_name);
    if (v != nullptr) {
      if (v->type == SyntheticValue::Type::kDouble) {
        *out_value = v->double_value;
      } else if (v->type == SyntheticValue::Type::kInt) {
        *out_value = static_cast<double>(v->int_value);
      }
    }
    last_error_.clear();
    return OFGDB_OK;
  }

  int row_get_blob(uint64_t row_handle, const char* column_name, uint8_t** out_data, int32_t* out_size) override {
    if (column_name == nullptr || out_data == nullptr || out_size == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "column/output missing");
    }
    *out_data = nullptr;
    *out_size = 0;
    std::lock_guard<std::mutex> lock(mutex_);
    RowState* row = get_row_locked(row_handle);
    if (row == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown row handle");
    }
    if (row->kind != RowKind::kFeature || row->feature == nullptr) {
      last_error_.clear();
      return OFGDB_OK;
    }
    int idx = find_field_index_ci(OGR_F_GetDefnRef(row->feature), column_name);
    if (idx >= 0) {
      if (!OGR_F_IsFieldSetAndNotNull(row->feature, idx)) {
        last_error_.clear();
        return OFGDB_OK;
      }
      OGRFieldDefnH fld = OGR_F_GetFieldDefnRef(row->feature, idx);
      if (fld == nullptr) {
        return fail(OFGDB_ERR_INTERNAL, "failed to inspect field definition");
      }
      if (OGR_Fld_GetType(fld) != OFTBinary) {
        return fail(OFGDB_ERR_INVALID_ARG, "row value is not blob");
      }
      int size = 0;
      const GByte* data = OGR_F_GetFieldAsBinary(row->feature, idx, &size);
      if (size < 0 || size > std::numeric_limits<int32_t>::max()) {
        return fail(OFGDB_ERR_INTERNAL, "blob too large");
      }
      *out_size = size;
      if (size > 0 && data != nullptr) {
        *out_data = dup_bytes(data, size);
        if (*out_data == nullptr) {
          return fail(OFGDB_ERR_INTERNAL, "out of memory");
        }
      }
      last_error_.clear();
      return OFGDB_OK;
    }
    int geom_idx = resolve_geom_field_index_for_row_locked(row, column_name);
    if (geom_idx >= 0) {
      OGRGeometryH raw_geom = OGR_F_GetGeomFieldRef(row->feature, geom_idx);
      GeometryContractKind declared_kind = get_declared_geom_kind_for_row_locked(row, geom_idx);
      OGRGeometryH geom = nullptr;
      std::string geometry_error;
      if (!normalize_geometry_for_row_read_locked(row, geom_idx, raw_geom, &geom, &geometry_error)) {
        return fail(OFGDB_ERR_INTERNAL, geometry_error);
      }
      if (geom == nullptr) {
        last_error_.clear();
        return OFGDB_OK;
      }
      int wkb_size = OGR_G_WkbSize(geom);
      if (wkb_size < 0 || wkb_size > std::numeric_limits<int32_t>::max()) {
        OGR_G_DestroyGeometry(geom);
        return fail(OFGDB_ERR_INTERNAL, "geometry too large");
      }
      if (wkb_size > 0) {
        uint8_t* buffer = static_cast<uint8_t*>(std::malloc(static_cast<size_t>(wkb_size)));
        if (buffer == nullptr) {
          OGR_G_DestroyGeometry(geom);
          return fail(OFGDB_ERR_INTERNAL, "out of memory");
        }
        OGRErr err = OGR_G_ExportToWkb(geom, wkbNDR, buffer);
        if (err != OGRERR_NONE) {
          OGR_G_DestroyGeometry(geom);
          std::free(buffer);
          return fail_from_cpl(map_ogr_error(err), "failed to export geometry to WKB");
        }
        *out_data = buffer;
      }
      OGR_G_DestroyGeometry(geom);
      *out_size = wkb_size;
      last_error_.clear();
      return OFGDB_OK;
    }
    last_error_.clear();
    return OFGDB_OK;
  }

  int row_get_geometry(uint64_t row_handle, uint8_t** out_wkb, int32_t* out_size) override {
    if (out_wkb == nullptr || out_size == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "output missing");
    }
    *out_wkb = nullptr;
    *out_size = 0;
    std::lock_guard<std::mutex> lock(mutex_);
    RowState* row = get_row_locked(row_handle);
    if (row == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown row handle");
    }
    if (row->kind != RowKind::kFeature || row->feature == nullptr || OGR_F_GetGeomFieldCount(row->feature) <= 0) {
      last_error_.clear();
      return OFGDB_OK;
    }
    OGRGeometryH raw_geom = OGR_F_GetGeomFieldRef(row->feature, 0);
    OGRGeometryH geom = nullptr;
    std::string geometry_error;
    if (!normalize_geometry_for_row_read_locked(row, 0, raw_geom, &geom, &geometry_error)) {
      return fail(OFGDB_ERR_INTERNAL, geometry_error);
    }
    if (geom == nullptr) {
      last_error_.clear();
      return OFGDB_OK;
    }
    int wkb_size = OGR_G_WkbSize(geom);
    if (wkb_size < 0 || wkb_size > std::numeric_limits<int32_t>::max()) {
      OGR_G_DestroyGeometry(geom);
      return fail(OFGDB_ERR_INTERNAL, "geometry too large");
    }
    if (wkb_size > 0) {
      uint8_t* buffer = static_cast<uint8_t*>(std::malloc(static_cast<size_t>(wkb_size)));
      if (buffer == nullptr) {
        OGR_G_DestroyGeometry(geom);
        return fail(OFGDB_ERR_INTERNAL, "out of memory");
      }
      OGRErr err = OGR_G_ExportToWkb(geom, wkbNDR, buffer);
      if (err != OGRERR_NONE) {
        OGR_G_DestroyGeometry(geom);
        std::free(buffer);
        return fail_from_cpl(map_ogr_error(err), "failed to export geometry to WKB");
      }
      *out_wkb = buffer;
    }
    OGR_G_DestroyGeometry(geom);
    *out_size = wkb_size;
    last_error_.clear();
    return OFGDB_OK;
  }

 private:
  struct DbState {
    GDALDatasetH dataset = nullptr;
    std::string path;
    std::unordered_map<std::string, std::unordered_map<std::string, GeometryContractKind>> geometry_contracts;
  };

  struct TableState {
    uint64_t db_handle = 0;
    std::string layer_name;
  };

  enum class CursorKind {
    kLayer,
    kLayerResultSet,
    kSyntheticRows,
  };

  enum class RowKind {
    kFeature,
    kSynthetic,
  };

  struct SyntheticValue {
    enum class Type {
      kNull,
      kString,
      kInt,
      kDouble,
    };

    Type type = Type::kNull;
    std::string string_value;
    int32_t int_value = 0;
    double double_value = 0.0;

    static SyntheticValue from_string(const std::string& v) {
      SyntheticValue value;
      value.type = Type::kString;
      value.string_value = v;
      return value;
    }
  };

  struct CursorState {
    uint64_t db_handle = 0;
    std::string layer_name;
    CursorKind kind = CursorKind::kLayer;
    OGRLayerH layer = nullptr;
    bool release_result_set = false;
    std::vector<std::unordered_map<std::string, SyntheticValue>> synthetic_rows;
    size_t synthetic_index = 0;
  };

  struct RowState {
    uint64_t db_handle = 0;
    std::string layer_name;
    RowKind kind = RowKind::kFeature;
    OGRFeatureH feature = nullptr;
    bool owns_feature = false;
    std::unordered_map<std::string, SyntheticValue> synthetic_values;
  };

  struct FieldInfoState {
    uint64_t db_handle = 0;
    std::vector<std::string> names;
  };

  static const SyntheticValue* get_synthetic_value_ci(
      const std::unordered_map<std::string, SyntheticValue>& values,
      const char* key) {
    if (key == nullptr) {
      return nullptr;
    }
    auto exact = values.find(key);
    if (exact != values.end()) {
      return &(exact->second);
    }
    std::string wanted = to_upper_copy(key);
    for (const auto& entry : values) {
      if (to_upper_copy(entry.first) == wanted) {
        return &(entry.second);
      }
    }
    return nullptr;
  }

  bool ensure_geometry_contract_registry_layer_locked(DbState& db, std::string* out_error) {
    if (out_error != nullptr) {
      out_error->clear();
    }
    OGRLayerH existing = GDALDatasetGetLayerByName(db.dataset, kGeomContractRegistryTable);
    if (existing != nullptr) {
      return true;
    }
    char** layer_options = nullptr;
    layer_options = CSLAddString(layer_options, "TARGET_ARCGIS_VERSION=ARCGIS_PRO_3_2_OR_LATER");
    OGRLayerH registry_layer = GDALDatasetCreateLayer(db.dataset, kGeomContractRegistryTable, nullptr, wkbNone, layer_options);
    CSLDestroy(layer_options);
    if (registry_layer == nullptr) {
      if (out_error != nullptr) {
        *out_error = "failed to create geometry contract registry layer";
      }
      return false;
    }
    struct FieldSpec {
      const char* name;
      OGRFieldType type;
    };
    const FieldSpec fields[] = {
        {kGeomContractRegistryTableField, OFTString},
        {kGeomContractRegistryColumnField, OFTString},
        {kGeomContractRegistryKindField, OFTString},
    };
    for (const FieldSpec& field : fields) {
      OGRFieldDefnH fld = OGR_Fld_Create(field.name, field.type);
      if (fld == nullptr) {
        if (out_error != nullptr) {
          *out_error = "failed to allocate geometry contract registry field";
        }
        return false;
      }
      OGRErr err = OGR_L_CreateField(registry_layer, fld, TRUE);
      OGR_Fld_Destroy(fld);
      if (err != OGRERR_NONE) {
        if (out_error != nullptr) {
          *out_error = "failed to create geometry contract registry field";
        }
        return false;
      }
    }
    return true;
  }

  bool upsert_geometry_contract_registry_locked(
      DbState& db,
      const std::string& table_name,
      const GeometrySqlColumnSpec& geometry_spec,
      std::string* out_error) {
    if (out_error != nullptr) {
      out_error->clear();
    }
    if (!ensure_geometry_contract_registry_layer_locked(db, out_error)) {
      return false;
    }
    OGRLayerH registry_layer = GDALDatasetGetLayerByName(db.dataset, kGeomContractRegistryTable);
    if (registry_layer == nullptr) {
      if (out_error != nullptr) {
        *out_error = "geometry contract registry layer not found";
      }
      return false;
    }
    OGRFeatureDefnH defn = OGR_L_GetLayerDefn(registry_layer);
    if (defn == nullptr) {
      if (out_error != nullptr) {
        *out_error = "failed to inspect geometry contract registry definition";
      }
      return false;
    }
    int table_idx = find_field_index_ci(defn, kGeomContractRegistryTableField);
    int column_idx = find_field_index_ci(defn, kGeomContractRegistryColumnField);
    int kind_idx = find_field_index_ci(defn, kGeomContractRegistryKindField);
    if (table_idx < 0 || column_idx < 0 || kind_idx < 0) {
      if (out_error != nullptr) {
        *out_error = "geometry contract registry schema is incomplete";
      }
      return false;
    }
    const char* kind_name = geometry_contract_kind_name(geometry_spec.contract_kind);
    if (kind_name == nullptr || *kind_name == '\0') {
      if (out_error != nullptr) {
        *out_error = "geometry contract registry kind is invalid";
      }
      return false;
    }
    OGRFeatureH feature = OGR_F_Create(defn);
    if (feature == nullptr) {
      if (out_error != nullptr) {
        *out_error = "failed to allocate geometry contract registry row";
      }
      return false;
    }
    OGR_F_SetFieldString(feature, table_idx, table_name.c_str());
    OGR_F_SetFieldString(feature, column_idx, geometry_spec.name.c_str());
    OGR_F_SetFieldString(feature, kind_idx, kind_name);
    OGRErr err = OGR_L_CreateFeature(registry_layer, feature);
    OGR_F_Destroy(feature);
    if (err != OGRERR_NONE) {
      if (out_error != nullptr) {
        *out_error = "failed to insert geometry contract registry row";
      }
      return false;
    }
    return true;
  }

  bool load_geometry_contracts_from_registry_locked(DbState& db, std::string* out_error) {
    if (out_error != nullptr) {
      out_error->clear();
    }
    OGRLayerH registry_layer = GDALDatasetGetLayerByName(db.dataset, kGeomContractRegistryTable);
    if (registry_layer == nullptr) {
      return true;
    }
    OGRFeatureDefnH defn = OGR_L_GetLayerDefn(registry_layer);
    if (defn == nullptr) {
      if (out_error != nullptr) {
        *out_error = "failed to inspect geometry contract registry definition";
      }
      return false;
    }
    int table_idx = find_field_index_ci(defn, kGeomContractRegistryTableField);
    int column_idx = find_field_index_ci(defn, kGeomContractRegistryColumnField);
    int kind_idx = find_field_index_ci(defn, kGeomContractRegistryKindField);
    if (table_idx < 0 || column_idx < 0 || kind_idx < 0) {
      if (out_error != nullptr) {
        *out_error = "geometry contract registry schema is incomplete";
      }
      return false;
    }
    struct RegistryContractEntry {
      GIntBig fid = -1;
      std::string table_upper;
      std::string column_upper;
      GeometryContractKind kind = GeometryContractKind::kUnknown;
    };
    std::unordered_map<std::string, RegistryContractEntry> latest_entries;
    OGR_L_ResetReading(registry_layer);
    while (true) {
      OGRFeatureH feature = OGR_L_GetNextFeature(registry_layer);
      if (feature == nullptr) {
        break;
      }
      GIntBig fid = OGR_F_GetFID(feature);
      std::string table_name;
      std::string column_name;
      std::string kind_name;
      if (OGR_F_IsFieldSetAndNotNull(feature, table_idx)) {
        table_name = OGR_F_GetFieldAsString(feature, table_idx);
      }
      if (OGR_F_IsFieldSetAndNotNull(feature, column_idx)) {
        column_name = OGR_F_GetFieldAsString(feature, column_idx);
      }
      if (OGR_F_IsFieldSetAndNotNull(feature, kind_idx)) {
        kind_name = OGR_F_GetFieldAsString(feature, kind_idx);
      }
      OGR_F_Destroy(feature);
      if (table_name.empty() || column_name.empty() || kind_name.empty()) {
        continue;
      }
      GeometryContractKind contract_kind = parse_geometry_contract_kind(kind_name);
      if (contract_kind == GeometryContractKind::kUnknown) {
        if (out_error != nullptr) {
          *out_error = "invalid geometry contract registry kind: ";
          *out_error += kind_name;
        }
        return false;
      }
      if (is_internal_table_name(table_name)) {
        continue;
      }
      if (GDALDatasetGetLayerByName(db.dataset, table_name.c_str()) == nullptr) {
        continue;
      }
      std::string table_upper = to_upper_copy(table_name);
      std::string column_upper = to_upper_copy(column_name);
      std::string key = table_upper;
      key.push_back('\x1F');
      key += column_upper;
      auto it = latest_entries.find(key);
      if (it == latest_entries.end() || fid >= it->second.fid) {
        latest_entries[key] = RegistryContractEntry{
            fid,
            std::move(table_upper),
            std::move(column_upper),
            contract_kind,
        };
      }
    }
    for (const auto& it : latest_entries) {
      const RegistryContractEntry& entry = it.second;
      db.geometry_contracts[entry.table_upper][entry.column_upper] = entry.kind;
    }
    return true;
  }

  bool load_geometry_contracts_locked(DbState& db, std::string* out_error) {
    if (out_error != nullptr) {
      out_error->clear();
    }
    db.geometry_contracts.clear();
    if (db.dataset == nullptr) {
      if (out_error != nullptr) {
        *out_error = "failed to load geometry contracts: dataset is null";
      }
      return false;
    }
    int layer_count = GDALDatasetGetLayerCount(db.dataset);
    for (int i = 0; i < layer_count; i++) {
      OGRLayerH layer = GDALDatasetGetLayer(db.dataset, i);
      if (layer == nullptr) {
        continue;
      }
      const char* layer_name = OGR_L_GetName(layer);
      if (layer_name == nullptr || *layer_name == '\0') {
        continue;
      }
      if (is_internal_table_name(layer_name)) {
        continue;
      }
      GDALMajorObjectH major_object = reinterpret_cast<GDALMajorObjectH>(layer);
      const char* column_name = GDALGetMetadataItem(major_object, kGeomContractColMetadataKey, nullptr);
      const char* kind_name = GDALGetMetadataItem(major_object, kGeomContractKindMetadataKey, nullptr);
      if (column_name == nullptr || *column_name == '\0' || kind_name == nullptr || *kind_name == '\0') {
        continue;
      }
      GeometryContractKind contract_kind = parse_geometry_contract_kind(kind_name);
      if (contract_kind == GeometryContractKind::kUnknown) {
        if (out_error != nullptr) {
          *out_error = "invalid persisted geometry contract kind for layer ";
          *out_error += layer_name;
          *out_error += ": ";
          *out_error += kind_name;
        }
        return false;
      }
      db.geometry_contracts[to_upper_copy(layer_name)][to_upper_copy(column_name)] = contract_kind;
    }
    if (!load_geometry_contracts_from_registry_locked(db, out_error)) {
      return false;
    }
    return true;
  }

  bool persist_geometry_contract_metadata_locked(
      OGRLayerH layer,
      const GeometrySqlColumnSpec& geometry_spec,
      std::string* out_error) {
    if (out_error != nullptr) {
      out_error->clear();
    }
    if (layer == nullptr) {
      if (out_error != nullptr) {
        *out_error = "failed to persist geometry contract metadata: layer is null";
      }
      return false;
    }
    const char* kind_name = geometry_contract_kind_name(geometry_spec.contract_kind);
    if (kind_name == nullptr || *kind_name == '\0') {
      if (out_error != nullptr) {
        *out_error = "failed to persist geometry contract metadata: unknown contract kind";
      }
      return false;
    }
    GDALMajorObjectH major_object = reinterpret_cast<GDALMajorObjectH>(layer);
    CPLErr err = GDALSetMetadataItem(major_object, kGeomContractColMetadataKey, geometry_spec.name.c_str(), nullptr);
    if (err != CE_None) {
      if (out_error != nullptr) {
        *out_error = "failed to persist geometry contract metadata: ";
        *out_error += kGeomContractColMetadataKey;
      }
      return false;
    }
    err = GDALSetMetadataItem(major_object, kGeomContractKindMetadataKey, kind_name, nullptr);
    if (err != CE_None) {
      if (out_error != nullptr) {
        *out_error = "failed to persist geometry contract metadata: ";
        *out_error += kGeomContractKindMetadataKey;
      }
      return false;
    }
    return true;
  }

  GeometryContractKind get_declared_geom_kind_for_table_column_locked(
      const DbState* db,
      const std::string& table_name,
      const char* column_name) const {
    if (db == nullptr || table_name.empty()) {
      return GeometryContractKind::kUnknown;
    }
    auto table_it = db->geometry_contracts.find(to_upper_copy(table_name));
    if (table_it == db->geometry_contracts.end()) {
      return GeometryContractKind::kUnknown;
    }
    if (column_name != nullptr && *column_name != '\0') {
      auto col_it = table_it->second.find(to_upper_copy(column_name));
      if (col_it != table_it->second.end()) {
        return col_it->second;
      }
    }
    if (table_it->second.size() == 1) {
      return table_it->second.begin()->second;
    }
    return GeometryContractKind::kUnknown;
  }

  int resolve_geom_field_index_for_table_locked(
      DbState* db,
      OGRFeatureDefnH defn,
      const std::string& table_name,
      const char* column_name) {
    if (defn == nullptr || column_name == nullptr || *column_name == '\0') {
      return -1;
    }
    int geom_idx = find_geom_field_index_ci(defn, column_name);
    if (geom_idx >= 0) {
      return geom_idx;
    }
    if (db == nullptr || table_name.empty()) {
      return -1;
    }
    auto table_it = db->geometry_contracts.find(to_upper_copy(table_name));
    if (table_it == db->geometry_contracts.end()) {
      return -1;
    }
    auto col_it = table_it->second.find(to_upper_copy(column_name));
    if (col_it == table_it->second.end()) {
      return -1;
    }
    return OGR_FD_GetGeomFieldCount(defn) == 1 ? 0 : -1;
  }

  int resolve_geom_field_index_for_row_locked(const RowState* row, const char* column_name) {
    if (row == nullptr || row->feature == nullptr) {
      return -1;
    }
    DbState* db = get_db_locked(row->db_handle);
    return resolve_geom_field_index_for_table_locked(db, OGR_F_GetDefnRef(row->feature), row->layer_name, column_name);
  }

  bool set_column_from_literal_locked(
      DbState* db,
      const std::string& table_name,
      OGRFeatureH feat,
      OGRFeatureDefnH defn,
      const std::string& column_name,
      const std::string& raw_value,
      std::string* out_error) {
    if (out_error != nullptr) {
      out_error->clear();
    }
    if (feat == nullptr || defn == nullptr) {
      if (out_error != nullptr) {
        *out_error = "feature definition missing";
      }
      return false;
    }
    int idx = find_field_index_ci(defn, column_name.c_str());
    if (idx >= 0) {
      OGRFieldDefnH fld = OGR_FD_GetFieldDefn(defn, idx);
      if (fld == nullptr) {
        if (out_error != nullptr) {
          *out_error = "failed to inspect field definition";
        }
        return false;
      }
      set_field_from_literal(feat, fld, idx, raw_value);
      return true;
    }
    int geom_idx = resolve_geom_field_index_for_table_locked(db, defn, table_name, column_name.c_str());
    if (geom_idx < 0) {
      if (out_error != nullptr) {
        *out_error = "unknown column";
      }
      return false;
    }
    std::string value = trim(raw_value);
    if (equals_ci(value, "NULL")) {
      return OGR_F_SetGeomField(feat, geom_idx, nullptr) == OGRERR_NONE;
    }
    if (!value.empty() && value.front() == '\'') {
      value = unquote(value);
    }
    std::vector<GByte> decoded;
    if (!decode_byte_literal_value(value, &decoded)) {
      if (out_error != nullptr) {
        *out_error = "invalid geometry byte literal";
      }
      return false;
    }
    if (decoded.empty()) {
      return OGR_F_SetGeomField(feat, geom_idx, nullptr) == OGRERR_NONE;
    }
    OGRGeometryH geom = nullptr;
    OGRErr err = OGR_G_CreateFromWkb(decoded.data(), nullptr, &geom, static_cast<int>(decoded.size()));
    if (err != OGRERR_NONE || geom == nullptr) {
      if (out_error != nullptr) {
        *out_error = "invalid WKB geometry";
      }
      return false;
    }
    GeometryContractKind declared_kind = get_declared_geom_kind_for_table_column_locked(db, table_name, column_name.c_str());
    if (declared_kind == GeometryContractKind::kUnknown) {
      declared_kind = get_declared_geom_kind(feat, geom_idx);
    }
    std::string geometry_error;
    if (!check_geometry_contract_kind(declared_kind, geom, &geometry_error)) {
      OGR_G_DestroyGeometry(geom);
      if (out_error != nullptr) {
        *out_error = geometry_error;
      }
      return false;
    }
    err = OGR_F_SetGeomFieldDirectly(feat, geom_idx, geom);
    if (err != OGRERR_NONE) {
      OGR_G_DestroyGeometry(geom);
      if (out_error != nullptr) {
        *out_error = "failed to set geometry field";
      }
      return false;
    }
    return true;
  }

  GeometryContractKind get_declared_geom_kind_for_row_locked(const RowState* row, int geom_idx) {
    if (row == nullptr || row->feature == nullptr || geom_idx < 0) {
      return GeometryContractKind::kUnknown;
    }
    DbState* db = get_db_locked(row->db_handle);
    if (db != nullptr) {
      auto table_it = db->geometry_contracts.find(to_upper_copy(row->layer_name));
      if (table_it != db->geometry_contracts.end()) {
        OGRFeatureDefnH defn = OGR_F_GetDefnRef(row->feature);
        if (defn != nullptr && geom_idx < OGR_FD_GetGeomFieldCount(defn)) {
          OGRGeomFieldDefnH geom_defn = OGR_FD_GetGeomFieldDefn(defn, geom_idx);
          if (geom_defn != nullptr) {
            const char* geom_name = OGR_GFld_GetNameRef(geom_defn);
            if (geom_name != nullptr) {
              auto col_it = table_it->second.find(to_upper_copy(geom_name));
              if (col_it != table_it->second.end()) {
                return col_it->second;
              }
            }
          }
        }
        if (table_it->second.size() == 1) {
          return table_it->second.begin()->second;
        }
        return GeometryContractKind::kUnknown;
      }
    }
    return get_declared_geom_kind(row->feature, geom_idx);
  }

  bool check_geometry_contract_for_row_locked(const RowState* row, int geom_idx, OGRGeometryH geom, std::string* out_error) {
    GeometryContractKind declared_kind = get_declared_geom_kind_for_row_locked(row, geom_idx);
    return check_geometry_contract_kind(declared_kind, geom, out_error);
  }

  bool normalize_geometry_for_row_read_locked(
      const RowState* row,
      int geom_idx,
      OGRGeometryH geom,
      OGRGeometryH* out_geom,
      std::string* out_error) {
    GeometryContractKind declared_kind = get_declared_geom_kind_for_row_locked(row, geom_idx);
    return normalize_geometry_for_read_kind(declared_kind, geom, out_geom, out_error);
  }

  static bool relationship_signature_matches(
      GDALRelationshipH relationship,
      const char* origin_table,
      const char* destination_table,
      const char* origin_pk,
      const char* origin_fk,
      const char* cardinality,
      bool composite,
      bool attributed) {
    if (relationship == nullptr) {
      return false;
    }
    const char* left_name = GDALRelationshipGetLeftTableName(relationship);
    const char* right_name = GDALRelationshipGetRightTableName(relationship);
    if (left_name == nullptr || right_name == nullptr) {
      return false;
    }
    if (!equals_ci(left_name, origin_table == nullptr ? "" : origin_table) ||
        !equals_ci(right_name, destination_table == nullptr ? "" : destination_table)) {
      return false;
    }

    char** left_fields = GDALRelationshipGetLeftTableFields(relationship);
    char** right_fields = GDALRelationshipGetRightTableFields(relationship);
    const char* left_field = (left_fields != nullptr && left_fields[0] != nullptr) ? left_fields[0] : "";
    const char* right_field = (right_fields != nullptr && right_fields[0] != nullptr) ? right_fields[0] : "";
    bool key_match = equals_ci(left_field, origin_pk == nullptr ? "" : origin_pk) &&
                     equals_ci(right_field, origin_fk == nullptr ? "" : origin_fk);
    CSLDestroy(left_fields);
    CSLDestroy(right_fields);
    if (!key_match) {
      return false;
    }

    GDALRelationshipCardinality rel_cardinality = GDALRelationshipGetCardinality(relationship);
    GDALRelationshipCardinality expected_cardinality = parse_cardinality(cardinality);
    if (rel_cardinality != expected_cardinality) {
      return false;
    }

    GDALRelationshipType rel_type = GDALRelationshipGetType(relationship);
    if (composite && rel_type != GRT_COMPOSITE) {
      return false;
    }
    if (!composite && rel_type == GRT_COMPOSITE) {
      return false;
    }

    const char* mapping_table = GDALRelationshipGetMappingTableName(relationship);
    bool has_mapping = mapping_table != nullptr && *mapping_table != '\0';
    if (attributed != has_mapping) {
      return false;
    }
    return true;
  }

  static const char* gdal_runtime_library_hint() {
    const char* stage = std::getenv("OPENFGDB4J_GDAL_MINIMAL_ROOT");
    if (stage != nullptr && *stage != '\0') {
      return stage;
    }
    const char* hint = std::getenv("OPENFGDB4J_GDAL_LIBRARY");
    if (hint != nullptr && *hint != '\0') {
      return hint;
    }
    return "static";
  }

  bool should_force_fail() const {
    return env_true(std::getenv("OPENFGDB4J_GDAL_FORCE_FAIL"));
  }

  bool relationship_signature_exists_locked(
      GDALDatasetH dataset,
      const char* origin_table,
      const char* destination_table,
      const char* origin_pk,
      const char* origin_fk,
      const char* cardinality,
      bool composite,
      bool attributed) {
    char** relationship_names = GDALDatasetGetRelationshipNames(dataset, nullptr);
    if (relationship_names == nullptr) {
      return false;
    }
    int count = CSLCount(relationship_names);
    for (int i = 0; i < count; i++) {
      const char* rel_name = relationship_names[i];
      if (rel_name == nullptr || *rel_name == '\0') {
        continue;
      }
      GDALRelationshipH rel = GDALDatasetGetRelationship(dataset, rel_name);
      if (relationship_signature_matches(rel, origin_table, destination_table, origin_pk, origin_fk, cardinality, composite, attributed)) {
        CSLDestroy(relationship_names);
        return true;
      }
    }
    CSLDestroy(relationship_names);
    return false;
  }

  uint64_t allocate_handle_locked() {
    return next_handle_++;
  }

  DbState* get_db_locked(uint64_t db_handle) {
    auto it = dbs_.find(db_handle);
    return it == dbs_.end() ? nullptr : &(it->second);
  }

  TableState* get_table_locked(uint64_t table_handle) {
    auto it = tables_.find(table_handle);
    return it == tables_.end() ? nullptr : &(it->second);
  }

  CursorState* get_cursor_locked(uint64_t cursor_handle) {
    auto it = cursors_.find(cursor_handle);
    return it == cursors_.end() ? nullptr : &(it->second);
  }

  RowState* get_row_locked(uint64_t row_handle) {
    auto it = rows_.find(row_handle);
    return it == rows_.end() ? nullptr : &(it->second);
  }

  int fail(int code, const std::string& message) {
    last_error_ = message;
    return code;
  }

  int fail_from_cpl(int code, const std::string& prefix) {
    int cpl_no = CPLGetLastErrorNo();
    CPLErr cpl_type = CPLGetLastErrorType();
    const char* cpl_msg = CPLGetLastErrorMsg();
    if (cpl_msg == nullptr || *cpl_msg == '\0') {
      return fail(code, prefix);
    }
    return fail(code, prefix + ": " + cpl_msg);
  }

  void destroy_cursor_locked(CursorState& cursor) {
    if (cursor.release_result_set && cursor.layer != nullptr) {
      DbState* db = get_db_locked(cursor.db_handle);
      if (db != nullptr && db->dataset != nullptr) {
        GDALDatasetReleaseResultSet(db->dataset, cursor.layer);
      }
    }
    cursor.layer = nullptr;
    cursor.synthetic_rows.clear();
  }

  void destroy_row_locked(RowState& row) {
    if (row.owns_feature && row.feature != nullptr) {
      OGR_F_Destroy(row.feature);
    }
    row.feature = nullptr;
    row.synthetic_values.clear();
  }

  int exec_sql_native_locked(DbState& db, const char* sql) {
    CPLErrorReset();
    OGRLayerH result = GDALDatasetExecuteSQL(db.dataset, sql, nullptr, nullptr);
    if (result != nullptr) {
      GDALDatasetReleaseResultSet(db.dataset, result);
      return OFGDB_OK;
    }
    CPLErr err_type = CPLGetLastErrorType();
    if (err_type == CE_None || err_type == CE_Warning) {
      return OFGDB_OK;
    }
    return OFGDB_ERR_INTERNAL;
  }

  int exec_sql_fallback_locked(DbState& db, const char* sql, uint64_t db_handle) {
    std::string stmt = trim(sql != nullptr ? sql : "");
    std::string stmt_upper = to_upper_copy(stmt);

    if (stmt_upper.rfind("CREATE TABLE", 0) == 0) {
      size_t name_start = std::strlen("CREATE TABLE");
      size_t paren = stmt.find('(', name_start);
      if (paren == std::string::npos) {
        return fail(OFGDB_ERR_INVALID_ARG, "CREATE TABLE syntax unsupported");
      }
      std::string table_name = trim(stmt.substr(name_start, paren - name_start));
      if (is_internal_table_name(table_name)) {
        return fail(OFGDB_ERR_INVALID_ARG, "table name is reserved");
      }
      size_t close = stmt.rfind(')');
      if (close == std::string::npos || close <= paren) {
        return fail(OFGDB_ERR_INVALID_ARG, "CREATE TABLE syntax unsupported");
      }
      std::string defs = stmt.substr(paren + 1, close - paren - 1);
      OGRLayerH existing = GDALDatasetGetLayerByName(db.dataset, table_name.c_str());
      if (existing != nullptr) {
        return OFGDB_OK;
      }

      std::vector<std::string> attribute_defs;
      std::vector<GeometrySqlColumnSpec> geometry_defs;
      for (const std::string& def : split_sql_top_level(defs)) {
        std::string trimmed_def = trim(def);
        if (trimmed_def.empty()) {
          continue;
        }
        std::vector<std::string> parts = split_ws_tokens(trimmed_def);
        if (parts.size() < 2) {
          continue;
        }
        std::string first_token = to_upper_copy(parts[0]);
        if (first_token == "PRIMARY" || first_token == "FOREIGN" || first_token == "CONSTRAINT" ||
            first_token == "UNIQUE" || first_token == "CHECK") {
          continue;
        }
        GeometrySqlColumnSpec geometry_spec;
        bool declared_geometry = starts_with_ci(parts[1], "OFGDB_GEOMETRY(");
        if (declared_geometry) {
          if (!parse_ofgdb_geometry_type(parts[0], parts[1], trimmed_def, &geometry_spec)) {
            return fail(
                OFGDB_ERR_INVALID_ARG,
                "invalid OFGDB_GEOMETRY definition (allowed: POINT, MULTIPOINT, LINE, LINESTRING, CIRCULARSTRING, COMPOUNDCURVE, MULTILINE, MULTILINESTRING, MULTICURVE, POLYGON, CURVEPOLYGON, MULTIPOLYGON, MULTISURFACE): " +
                    trimmed_def);
          }
          geometry_defs.push_back(geometry_spec);
        } else {
          attribute_defs.push_back(trimmed_def);
        }
      }
      if (geometry_defs.size() > 1) {
        return fail(OFGDB_ERR_INVALID_ARG, "CREATE TABLE with more than one OFGDB_GEOMETRY column is not supported");
      }

      char** layer_options = nullptr;
      layer_options = CSLAddString(layer_options, "TARGET_ARCGIS_VERSION=ARCGIS_PRO_3_2_OR_LATER");
      OGRwkbGeometryType layer_geom_type = wkbNone;
      OGRSpatialReferenceH layer_srs = nullptr;
      if (!geometry_defs.empty()) {
        const GeometrySqlColumnSpec& geom = geometry_defs.front();
        layer_geom_type = geom.ogr_type;
        std::string geometry_name_opt = std::string("GEOMETRY_NAME=") + geom.name;
        layer_options = CSLAddString(layer_options, geometry_name_opt.c_str());
        if (!geom.nullable) {
          layer_options = CSLAddString(layer_options, "GEOMETRY_NULLABLE=NO");
        }
        if (geom.epsg > 0) {
          layer_srs = OSRNewSpatialReference(nullptr);
          if (layer_srs == nullptr || OSRImportFromEPSG(layer_srs, geom.epsg) != OGRERR_NONE) {
            if (layer_srs != nullptr) {
              OSRDestroySpatialReference(layer_srs);
            }
            CSLDestroy(layer_options);
            return fail(OFGDB_ERR_INVALID_ARG, "invalid EPSG code in OFGDB_GEOMETRY definition");
          }
        }
      }
      OGRLayerH layer = GDALDatasetCreateLayer(db.dataset, table_name.c_str(), layer_srs, layer_geom_type, layer_options);
      if (layer_srs != nullptr) {
        OSRDestroySpatialReference(layer_srs);
      }
      CSLDestroy(layer_options);
      if (layer == nullptr) {
        return fail_from_cpl(OFGDB_ERR_INTERNAL, "failed to create table");
      }
      for (const std::string& attribute_def : attribute_defs) {
        std::vector<std::string> parts = split_ws_tokens(attribute_def);
        if (parts.size() < 2) {
          continue;
        }
        std::string col_name = parts[0];
        std::string type_name = parts[1];
        OGRFieldType field_type = map_field_type_from_column_definition(type_name, attribute_def);
        OGRFieldDefnH fld = OGR_Fld_Create(col_name.c_str(), field_type);
        if (fld == nullptr) {
          return fail(OFGDB_ERR_INTERNAL, "failed to allocate field definition");
        }
        OGRErr err = OGR_L_CreateField(layer, fld, TRUE);
        OGR_Fld_Destroy(fld);
        if (err != OGRERR_NONE) {
          return fail_from_cpl(map_ogr_error(err), "failed to create field");
        }
      }
      if (!geometry_defs.empty()) {
        std::unordered_map<std::string, GeometryContractKind> table_contracts;
        for (const GeometrySqlColumnSpec& geom : geometry_defs) {
          table_contracts[to_upper_copy(geom.name)] = geom.contract_kind;
        }
        db.geometry_contracts[to_upper_copy(table_name)] = std::move(table_contracts);
        std::string persist_error;
        if (!persist_geometry_contract_metadata_locked(layer, geometry_defs.front(), &persist_error)) {
          return fail_from_cpl(OFGDB_ERR_INTERNAL, persist_error);
        }
        if (!upsert_geometry_contract_registry_locked(db, table_name, geometry_defs.front(), &persist_error)) {
          return fail_from_cpl(OFGDB_ERR_INTERNAL, persist_error);
        }
      } else {
        db.geometry_contracts.erase(to_upper_copy(table_name));
      }
      last_error_.clear();
      return OFGDB_OK;
    }

    if (stmt_upper.rfind("DROP TABLE", 0) == 0) {
      std::string table_name = trim(stmt.substr(std::strlen("DROP TABLE")));
      if (table_name.empty()) {
        return fail(OFGDB_ERR_INVALID_ARG, "DROP TABLE syntax unsupported");
      }
      if (is_internal_table_name(table_name)) {
        return fail(OFGDB_ERR_INVALID_ARG, "table name is reserved");
      }
      int layer_count = GDALDatasetGetLayerCount(db.dataset);
      int layer_index = -1;
      for (int i = 0; i < layer_count; i++) {
        OGRLayerH layer = GDALDatasetGetLayer(db.dataset, i);
        if (layer == nullptr) {
          continue;
        }
        const char* layer_name = OGR_L_GetName(layer);
        if (layer_name != nullptr && equals_ci(layer_name, table_name)) {
          layer_index = i;
          break;
        }
      }
      if (layer_index < 0) {
        last_error_.clear();
        return OFGDB_OK;
      }
      CPLErrorReset();
      OGRErr err = GDALDatasetDeleteLayer(db.dataset, layer_index);
      if (err != OGRERR_NONE) {
        return fail_from_cpl(OFGDB_ERR_INTERNAL, "failed to drop table");
      }
      db.geometry_contracts.erase(to_upper_copy(table_name));
      last_error_.clear();
      return OFGDB_OK;
    }

    if (stmt_upper.rfind("DELETE FROM", 0) == 0) {
      size_t from_start = std::strlen("DELETE FROM");
      size_t where_pos = stmt_upper.find(" WHERE ", from_start);
      std::string table_name;
      std::string where_clause;
      if (where_pos == std::string::npos) {
        table_name = trim(stmt.substr(from_start));
      } else {
        table_name = trim(stmt.substr(from_start, where_pos - from_start));
        where_clause = trim(stmt.substr(where_pos + 7));
      }
      OGRLayerH layer = GDALDatasetGetLayerByName(db.dataset, table_name.c_str());
      if (layer == nullptr) {
        return fail(OFGDB_ERR_NOT_FOUND, "table does not exist");
      }
      OGR_L_SetAttributeFilter(layer, where_clause.empty() ? nullptr : where_clause.c_str());
      OGR_L_ResetReading(layer);
      while (true) {
        OGRFeatureH feat = OGR_L_GetNextFeature(layer);
        if (feat == nullptr) {
          break;
        }
        OGRErr err = OGR_L_DeleteFeature(layer, OGR_F_GetFID(feat));
        OGR_F_Destroy(feat);
        if (err != OGRERR_NONE) {
          OGR_L_SetAttributeFilter(layer, nullptr);
          return fail_from_cpl(map_ogr_error(err), "failed to delete feature");
        }
      }
      OGR_L_SetAttributeFilter(layer, nullptr);
      last_error_.clear();
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
      size_t values_pos = stmt_upper.find("VALUES", col_close);
      if (values_pos == std::string::npos) {
        return fail(OFGDB_ERR_INVALID_ARG, "INSERT syntax unsupported");
      }
      size_t val_open = stmt.find('(', values_pos);
      size_t val_close = stmt.rfind(')');
      if (val_open == std::string::npos || val_close == std::string::npos || val_close <= val_open) {
        return fail(OFGDB_ERR_INVALID_ARG, "INSERT syntax unsupported");
      }
      OGRLayerH layer = GDALDatasetGetLayerByName(db.dataset, table_name.c_str());
      if (layer == nullptr) {
        return fail(OFGDB_ERR_NOT_FOUND, "table does not exist");
      }
      std::vector<std::string> cols = split(stmt.substr(col_open + 1, col_close - col_open - 1), ',');
      std::vector<std::string> values = split_sql_values(stmt.substr(val_open + 1, val_close - val_open - 1));
      if (cols.size() != values.size()) {
        return fail(OFGDB_ERR_INVALID_ARG, "column/value count mismatch");
      }
      OGRFeatureDefnH defn = OGR_L_GetLayerDefn(layer);
      OGRFeatureH feat = OGR_F_Create(defn);
      if (feat == nullptr) {
        return fail(OFGDB_ERR_INTERNAL, "failed to allocate feature");
      }
      for (size_t i = 0; i < cols.size(); i++) {
        std::string column_error;
        if (!set_column_from_literal_locked(&db, table_name, feat, defn, cols[i], values[i], &column_error)) {
          OGR_F_Destroy(feat);
          std::string message = std::string("unknown column or invalid value: ") + cols[i];
          if (!column_error.empty()) {
            message += " (" + column_error + ")";
          }
          return fail(OFGDB_ERR_INVALID_ARG, message);
        }
      }
      OGRErr err = OGR_L_CreateFeature(layer, feat);
      OGR_F_Destroy(feat);
      if (err != OGRERR_NONE) {
        return fail_from_cpl(map_ogr_error(err), "failed to insert feature");
      }
      last_error_.clear();
      return OFGDB_OK;
    }

    if (stmt_upper.rfind("UPDATE", 0) == 0) {
      size_t set_pos = stmt_upper.find(" SET ");
      if (set_pos == std::string::npos) {
        return fail(OFGDB_ERR_INVALID_ARG, "UPDATE syntax unsupported");
      }
      std::string table_name = trim(stmt.substr(std::strlen("UPDATE"), set_pos - std::strlen("UPDATE")));
      size_t where_pos = stmt_upper.find(" WHERE ", set_pos + 5);
      std::string set_clause = where_pos == std::string::npos ? trim(stmt.substr(set_pos + 5))
                                                               : trim(stmt.substr(set_pos + 5, where_pos - (set_pos + 5)));
      std::string where_clause = where_pos == std::string::npos ? "" : trim(stmt.substr(where_pos + 7));
      OGRLayerH layer = GDALDatasetGetLayerByName(db.dataset, table_name.c_str());
      if (layer == nullptr) {
        return fail(OFGDB_ERR_NOT_FOUND, "table does not exist");
      }
      std::vector<std::string> assignments = split_sql_values(set_clause);
      OGRFeatureDefnH defn = OGR_L_GetLayerDefn(layer);
      OGR_L_SetAttributeFilter(layer, where_clause.empty() ? nullptr : where_clause.c_str());
      OGR_L_ResetReading(layer);
      while (true) {
        OGRFeatureH feat = OGR_L_GetNextFeature(layer);
        if (feat == nullptr) {
          break;
        }
        for (const std::string& assignment : assignments) {
          size_t eq = assignment.find('=');
          if (eq == std::string::npos) {
            continue;
          }
          std::string col = trim(assignment.substr(0, eq));
          std::string raw_value = assignment.substr(eq + 1);
          std::string column_error;
          if (!set_column_from_literal_locked(&db, table_name, feat, defn, col, raw_value, &column_error)) {
            OGR_F_Destroy(feat);
            OGR_L_SetAttributeFilter(layer, nullptr);
            std::string message = std::string("unknown column or invalid value in UPDATE: ") + col;
            if (!column_error.empty()) {
              message += " (" + column_error + ")";
            }
            return fail(OFGDB_ERR_INVALID_ARG, message);
          }
        }
        OGRErr err = OGR_L_SetFeature(layer, feat);
        OGR_F_Destroy(feat);
        if (err != OGRERR_NONE) {
          OGR_L_SetAttributeFilter(layer, nullptr);
          return fail_from_cpl(map_ogr_error(err), "failed to update feature");
        }
      }
      OGR_L_SetAttributeFilter(layer, nullptr);
      last_error_.clear();
      return OFGDB_OK;
    }

    return fail_from_cpl(OFGDB_ERR_INTERNAL, "SQL execution failed");
  }

  int list_text_from_name_array(uint64_t db_handle, char** out_text, bool domain_names) {
    if (out_text == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "output pointer missing");
    }
    *out_text = nullptr;
    std::lock_guard<std::mutex> lock(mutex_);
    DbState* db = get_db_locked(db_handle);
    if (db == nullptr) {
      return fail(OFGDB_ERR_INVALID_ARG, "unknown db handle");
    }
    char** names = domain_names ? GDALDatasetGetFieldDomainNames(db->dataset, nullptr)
                                : GDALDatasetGetRelationshipNames(db->dataset, nullptr);
    std::vector<std::string> values;
    if (names != nullptr) {
      int count = CSLCount(names);
      values.reserve(static_cast<size_t>(count));
      for (int i = 0; i < count; i++) {
        if (names[i] != nullptr && *names[i] != '\0') {
          values.emplace_back(names[i]);
        }
      }
      CSLDestroy(names);
    }
    char* duplicated = dup_cstr(join_lines(values));
    if (duplicated == nullptr) {
      return fail(OFGDB_ERR_INTERNAL, "out of memory");
    }
    *out_text = duplicated;
    last_error_.clear();
    return OFGDB_OK;
  }

  std::mutex mutex_;
  uint64_t next_handle_ = 1;
  std::unordered_map<uint64_t, DbState> dbs_;
  std::unordered_map<uint64_t, TableState> tables_;
  std::unordered_map<uint64_t, CursorState> cursors_;
  std::unordered_map<uint64_t, RowState> rows_;
  std::unordered_map<uint64_t, FieldInfoState> field_infos_;
  std::string last_error_;
};

std::unique_ptr<OpenFgdbBackend> create_gdal_backend() {
  return std::unique_ptr<OpenFgdbBackend>(new GdalBackend());
}

}  // namespace openfgdb
