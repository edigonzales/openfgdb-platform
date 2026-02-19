cmake_minimum_required(VERSION 3.20)
project(openfgdb4j_native VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(MSVC AND NOT DEFINED CMAKE_MSVC_RUNTIME_LIBRARY)
  # Match gdal-minimal static libs (/MT) to avoid LNK2038 RuntimeLibrary mismatch.
  set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
endif()

set(GDAL_VENDOR_OPENFILEGDB_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../third_party/gdal_openfilegdb/src/ogr/ogrsf_frmts/openfilegdb")
set(GDAL_VENDOR_CORE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../third_party/gdal_core_minimal/src")
set(OPENFGDB4J_GDAL_MINIMAL_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../gdal-minimal/build/stage" CACHE PATH
    "Path to gdal-minimal stage directory (contains include/ and lib/)")

set(OPENFGDB4J_GDAL_INCLUDE_DIR "${OPENFGDB4J_GDAL_MINIMAL_ROOT}/include")
set(OPENFGDB4J_GDAL_INCLUDE_GDAL_DIR "${OPENFGDB4J_GDAL_MINIMAL_ROOT}/include/gdal")

function(resolve_stage_static_lib out_var base_name)
  set(candidates)
  if(WIN32)
    list(APPEND candidates
      "${OPENFGDB4J_GDAL_MINIMAL_ROOT}/lib/${base_name}.lib"
      "${OPENFGDB4J_GDAL_MINIMAL_ROOT}/lib/lib${base_name}.lib")
  else()
    list(APPEND candidates
      "${OPENFGDB4J_GDAL_MINIMAL_ROOT}/lib/lib${base_name}.a"
      "${OPENFGDB4J_GDAL_MINIMAL_ROOT}/lib/${base_name}.a")
  endif()

  foreach(candidate IN LISTS candidates)
    if(EXISTS "${candidate}")
      set(${out_var} "${candidate}" PARENT_SCOPE)
      return()
    endif()
  endforeach()

  message(FATAL_ERROR
    "Missing staged static library for ${base_name}. Looked in: ${candidates}\n"
    "Run gdal-minimal build scripts or set OPENFGDB4J_GDAL_MINIMAL_ROOT.")
endfunction()

resolve_stage_static_lib(OPENFGDB4J_GDAL_STATIC_LIB gdal)
resolve_stage_static_lib(OPENFGDB4J_PROJ_STATIC_LIB proj)
resolve_stage_static_lib(OPENFGDB4J_SQLITE_STATIC_LIB sqlite3)

if(NOT EXISTS "${OPENFGDB4J_GDAL_INCLUDE_DIR}")
  message(FATAL_ERROR
    "Missing GDAL include dir: ${OPENFGDB4J_GDAL_INCLUDE_DIR}\n"
    "Run gdal-minimal build scripts or set OPENFGDB4J_GDAL_MINIMAL_ROOT.")
endif()

option(OPENFGDB4J_ENABLE_GDAL_VENDOR_PROBE "Compile vendored OpenFileGDB core sources as build probe" ON)

message(STATUS "openfgdb4j static stage root: ${OPENFGDB4J_GDAL_MINIMAL_ROOT}")
message(STATUS "openfgdb4j static libs: ${OPENFGDB4J_GDAL_STATIC_LIB};${OPENFGDB4J_PROJ_STATIC_LIB};${OPENFGDB4J_SQLITE_STATIC_LIB}")

add_library(openfgdb SHARED
    src/openfgdb_c_api.cpp
    src/openfgdb_backend_dispatch.cpp
    src/openfgdb_backend_adapter.cpp
    src/openfgdb_backend_gdal.cpp
    src/openfgdb_native_adapter.cpp
    src/openfgdb_catalog.cpp
    src/openfgdb_tableio.cpp
    src/openfgdb_handles.cpp
)

target_compile_definitions(openfgdb PRIVATE OFGDB_BUILD_DLL=1)
set_target_properties(openfgdb PROPERTIES
    WINDOWS_EXPORT_ALL_SYMBOLS ON
)

target_include_directories(openfgdb
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE
    ${OPENFGDB4J_GDAL_INCLUDE_DIR}
    ${OPENFGDB4J_GDAL_INCLUDE_GDAL_DIR}
)

target_link_libraries(openfgdb PRIVATE
    "${OPENFGDB4J_GDAL_STATIC_LIB}"
    "${OPENFGDB4J_PROJ_STATIC_LIB}"
    "${OPENFGDB4J_SQLITE_STATIC_LIB}"
)

if(APPLE)
  target_link_libraries(openfgdb PRIVATE "-liconv")
  set_target_properties(openfgdb PROPERTIES INSTALL_NAME_DIR "@rpath")
elseif(UNIX)
  target_link_libraries(openfgdb PRIVATE m dl pthread)
elseif(WIN32)
  target_link_libraries(openfgdb PRIVATE ws2_32 user32 advapi32 shell32 ole32 uuid bcrypt)
endif()

if(OPENFGDB4J_ENABLE_GDAL_VENDOR_PROBE)
  add_library(openfgdb_gdal_vendor_probe OBJECT
      ${GDAL_VENDOR_OPENFILEGDB_DIR}/filegdbindex.cpp
      ${GDAL_VENDOR_OPENFILEGDB_DIR}/filegdbindex_write.cpp
      ${GDAL_VENDOR_OPENFILEGDB_DIR}/filegdbtable.cpp
      ${GDAL_VENDOR_OPENFILEGDB_DIR}/filegdbtable_write.cpp
      ${GDAL_VENDOR_OPENFILEGDB_DIR}/filegdbtable_write_fields.cpp
      ${GDAL_VENDOR_OPENFILEGDB_DIR}/filegdbtable_freelist.cpp
  )

  target_include_directories(openfgdb_gdal_vendor_probe
      PRIVATE
      ${GDAL_VENDOR_OPENFILEGDB_DIR}
      ${GDAL_VENDOR_CORE_DIR}
      ${GDAL_VENDOR_CORE_DIR}/port
      ${GDAL_VENDOR_CORE_DIR}/gcore
      ${GDAL_VENDOR_CORE_DIR}/ogr
      ${GDAL_VENDOR_CORE_DIR}/ogr/ogrsf_frmts
  )

  target_compile_definitions(openfgdb_gdal_vendor_probe PRIVATE GDAL_COMPILATION=1)
  if(MSVC)
    target_compile_options(openfgdb_gdal_vendor_probe PRIVATE /Gy)
  else()
    target_compile_options(openfgdb_gdal_vendor_probe PRIVATE -ffunction-sections -fdata-sections)
  endif()

  add_custom_target(openfgdb_gdal_vendor_probe_build DEPENDS openfgdb_gdal_vendor_probe)
  add_dependencies(openfgdb openfgdb_gdal_vendor_probe_build)
endif()
