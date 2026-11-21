#pragma once
// Zero-dependency, ultra-fast Parquet v1 writer (PLAIN, UNCOMPRESSED, Thrift Compact Protocol).
// Supports REQUIRED INT64, REQUIRED INT32 (DATE), REQUIRED BYTE_ARRAY (UTF8/STRING),
// and OPTIONAL BYTE_ARRAY (UTF8/STRING) with RLE/bit-packed definition levels.

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace pq {

// Thrift Compact Protocol encoder
struct CompactWriter {
    std::vector<uint8_t> buf;
    int16_t last_field_id = 0;
    std::vector<int16_t> id_stack;

    void write_byte(uint8_t b) { buf.push_back(b); }

    void write_uvarint(uint64_t v) {
        while (v >= 0x80) {
            buf.push_back(static_cast<uint8_t>((v & 0x7F) | 0x80));
            v >>= 7;
        }
        buf.push_back(static_cast<uint8_t>(v));
    }

    void write_zigzag32(int32_t v) {
        uint32_t u = (static_cast<uint32_t>(v) << 1) ^ static_cast<uint32_t>(v >> 31);
        write_uvarint(u);
    }

    void write_zigzag64(int64_t v) {
        uint64_t u = (static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63);
        write_uvarint(u);
    }

    void write_field_header(int16_t field_id, uint8_t type_id) {
        int16_t delta = field_id - last_field_id;
        if (delta > 0 && delta <= 15) {
            write_byte(static_cast<uint8_t>((delta << 4) | (type_id & 0x0F)));
        } else {
            write_byte(type_id & 0x0F);
            write_zigzag32(field_id);
        }
        last_field_id = field_id;
    }

    void write_struct_begin() {
        id_stack.push_back(last_field_id);
        last_field_id = 0;
    }

    void write_struct_end() {
        write_byte(0); // STOP
        last_field_id = id_stack.back();
        id_stack.pop_back();
    }

    void write_i32_field(int16_t fid, int32_t v) {
        write_field_header(fid, 5); // CT_I32
        write_zigzag32(v);
    }

    void write_i64_field(int16_t fid, int64_t v) {
        write_field_header(fid, 6); // CT_I64
        write_zigzag64(v);
    }

    void write_string_field(int16_t fid, std::string_view s) {
        write_field_header(fid, 8); // CT_BINARY
        write_uvarint(s.size());
        buf.insert(buf.end(), reinterpret_cast<const uint8_t*>(s.data()),
                   reinterpret_cast<const uint8_t*>(s.data()) + s.size());
    }

    void write_list_header(int16_t fid, uint8_t elem_type, uint32_t size) {
        write_field_header(fid, 9); // CT_LIST
        if (size < 15) {
            write_byte(static_cast<uint8_t>((size << 4) | (elem_type & 0x0F)));
        } else {
            write_byte(static_cast<uint8_t>(0xF0 | (elem_type & 0x0F)));
            write_uvarint(size);
        }
    }

    void write_struct_field(int16_t fid) {
        write_field_header(fid, 12); // CT_STRUCT
        write_struct_begin();
    }
};

enum class ColKind { INT64, DATE32, UTF8_REQ, UTF8_OPT };

struct ColSpec {
    std::string name;
    ColKind kind;
};

// Build RLE/bit-packed hybrid definition levels for bit_width = 1 (required by Parquet for OPTIONAL cols)
inline std::vector<uint8_t> encode_def_levels_bw1(const std::vector<uint8_t>& present) {
    // Use bit-packed runs of up to 63*8 = 504 values (header = (num_groups << 1) | 1)
    std::vector<uint8_t> encoded;
    size_t n = present.size();
    size_t i = 0;
    while (i < n) {
        size_t rem = n - i;
        size_t groups = (rem + 7) / 8;
        if (groups > 63) groups = 63;
        uint8_t header = static_cast<uint8_t>((groups << 1) | 1);
        encoded.push_back(header);
        for (size_t g = 0; g < groups; ++g) {
            uint8_t byte = 0;
            for (size_t b = 0; b < 8; ++b) {
                size_t idx = i + g * 8 + b;
                if (idx < n && present[idx]) {
                    byte |= static_cast<uint8_t>(1u << b);
                }
            }
            encoded.push_back(byte);
        }
        i += groups * 8;
    }
    std::vector<uint8_t> out;
    out.resize(4 + encoded.size());
    uint32_t len = static_cast<uint32_t>(encoded.size());
    std::memcpy(out.data(), &len, 4);
    if (!encoded.empty()) {
        std::memcpy(out.data() + 4, encoded.data(), encoded.size());
    }
    return out;
}

struct ColumnBuffer {
    ColSpec spec;
    std::vector<uint8_t> values;     // PLAIN-encoded values
    std::vector<uint8_t> present;    // only used for UTF8_OPT (1 = non-null, 0 = null)
    int64_t num_rows = 0;

    void append_i64(int64_t v) {
        size_t off = values.size();
        values.resize(off + 8);
        std::memcpy(values.data() + off, &v, 8);
        num_rows++;
    }

    void append_i32(int32_t v) {
        size_t off = values.size();
        values.resize(off + 4);
        std::memcpy(values.data() + off, &v, 4);
        num_rows++;
    }

    void append_str(std::string_view s) {
        uint32_t len = static_cast<uint32_t>(s.size());
        size_t off = values.size();
        values.resize(off + 4 + len);
        std::memcpy(values.data() + off, &len, 4);
        if (len > 0) std::memcpy(values.data() + off + 4, s.data(), len);
        num_rows++;
    }

    void append_opt_str(std::string_view s, bool is_present) {
        present.push_back(is_present ? 1 : 0);
        if (is_present) {
            uint32_t len = static_cast<uint32_t>(s.size());
            size_t off = values.size();
            values.resize(off + 4 + len);
            std::memcpy(values.data() + off, &len, 4);
            if (len > 0) std::memcpy(values.data() + off + 4, s.data(), len);
        }
        num_rows++;
    }
};

inline int32_t physical_type(ColKind k) {
    switch (k) {
        case ColKind::INT64: return 2;     // INT64
        case ColKind::DATE32: return 1;    // INT32
        case ColKind::UTF8_REQ: return 6;  // BYTE_ARRAY
        case ColKind::UTF8_OPT: return 6;  // BYTE_ARRAY
    }
    return 6;
}

inline void write_parquet_file(const std::string& path, const std::vector<ColumnBuffer>& cols) {
    FILE* f = std::fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot open output parquet: " + path);

    // Buffered output
    std::vector<char> io_buf(1 << 20);
    std::setvbuf(f, io_buf.data(), _IOFBF, io_buf.size());

    std::fwrite("PAR1", 1, 4, f);
    int64_t offset = 4;
    int64_t num_rows = cols.empty() ? 0 : cols[0].num_rows;

    struct ChunkMeta {
        int64_t data_page_offset;
        int64_t total_size;
    };
    std::vector<ChunkMeta> chunk_metas;
    int64_t rg_total_bytes = 0;

    if (num_rows > 0) {
        chunk_metas.reserve(cols.size());
        for (const auto& col : cols) {
            int64_t page_start = offset;
            std::vector<uint8_t> def_bytes;
            if (col.spec.kind == ColKind::UTF8_OPT) {
                def_bytes = encode_def_levels_bw1(col.present);
            }
            int32_t page_payload_size = static_cast<int32_t>(def_bytes.size() + col.values.size());

            // Encode PageHeader
            CompactWriter ph;
            ph.write_struct_begin();
            ph.write_i32_field(1, 0); // PageType::DATA_PAGE
            ph.write_i32_field(2, page_payload_size); // uncompressed_page_size
            ph.write_i32_field(3, page_payload_size); // compressed_page_size
            ph.write_struct_field(5); // data_page_header
            ph.write_i32_field(1, static_cast<int32_t>(num_rows)); // num_values
            ph.write_i32_field(2, 0); // Encoding::PLAIN
            ph.write_i32_field(3, 3); // Encoding::RLE (definition_level_encoding)
            ph.write_i32_field(4, 3); // Encoding::RLE (repetition_level_encoding)
            ph.write_struct_end();
            ph.write_struct_end();

            std::fwrite(ph.buf.data(), 1, ph.buf.size(), f);
            if (!def_bytes.empty()) {
                std::fwrite(def_bytes.data(), 1, def_bytes.size(), f);
            }
            if (!col.values.empty()) {
                std::fwrite(col.values.data(), 1, col.values.size(), f);
            }
            int64_t chunk_bytes = static_cast<int64_t>(ph.buf.size()) + page_payload_size;
            offset += chunk_bytes;
            rg_total_bytes += chunk_bytes;
            chunk_metas.push_back({page_start, chunk_bytes});
        }
    }

    // Encode FileMetaData
    CompactWriter fm;
    fm.write_struct_begin();
    fm.write_i32_field(1, 1); // version = 1

    // field 2: schema (root + N columns)
    fm.write_list_header(2, 12, static_cast<uint32_t>(1 + cols.size()));
    // Root SchemaElement
    fm.write_struct_begin();
    fm.write_string_field(4, "schema");
    fm.write_i32_field(5, static_cast<int32_t>(cols.size())); // num_children
    fm.write_struct_end();

    for (const auto& col : cols) {
        fm.write_struct_begin();
        fm.write_i32_field(1, physical_type(col.spec.kind)); // type
        // repetition_type: REQUIRED = 0, OPTIONAL = 1
        fm.write_i32_field(3, col.spec.kind == ColKind::UTF8_OPT ? 1 : 0);
        fm.write_string_field(4, col.spec.name);
        if (col.spec.kind == ColKind::UTF8_REQ || col.spec.kind == ColKind::UTF8_OPT) {
            fm.write_i32_field(6, 0); // ConvertedType::UTF8 = 0
            // LogicalType::STRING (field 10 -> union field 1 = StringType{})
            fm.write_struct_field(10);
            fm.write_struct_field(1);
            fm.write_struct_end();
            fm.write_struct_end();
        } else if (col.spec.kind == ColKind::DATE32) {
            fm.write_i32_field(6, 6); // ConvertedType::DATE = 6
            // LogicalType::DATE (field 10 -> union field 6 = DateType{})
            fm.write_struct_field(10);
            fm.write_struct_field(6);
            fm.write_struct_end();
            fm.write_struct_end();
        }
        fm.write_struct_end();
    }

    // field 3: num_rows
    fm.write_i64_field(3, num_rows);

    // field 4: row_groups
    if (num_rows > 0) {
        fm.write_list_header(4, 12, 1);
        fm.write_struct_begin(); // RowGroup
        // field 1: columns (list<ColumnChunk>)
        fm.write_list_header(1, 12, static_cast<uint32_t>(cols.size()));
        for (size_t i = 0; i < cols.size(); ++i) {
            fm.write_struct_begin(); // ColumnChunk
            fm.write_i64_field(2, chunk_metas[i].data_page_offset); // file_offset
            fm.write_struct_field(3); // ColumnMetaData
            fm.write_i32_field(1, physical_type(cols[i].spec.kind)); // type
            // field 2: encodings (list<i32>)
            if (cols[i].spec.kind == ColKind::UTF8_OPT) {
                fm.write_list_header(2, 5, 2);
                fm.write_zigzag32(0); // PLAIN
                fm.write_zigzag32(3); // RLE
            } else {
                fm.write_list_header(2, 5, 1);
                fm.write_zigzag32(0); // PLAIN
            }
            // field 3: path_in_schema (list<string>)
            fm.write_list_header(3, 8, 1);
            fm.write_uvarint(cols[i].spec.name.size());
            fm.buf.insert(fm.buf.end(),
                          reinterpret_cast<const uint8_t*>(cols[i].spec.name.data()),
                          reinterpret_cast<const uint8_t*>(cols[i].spec.name.data()) + cols[i].spec.name.size());
            fm.write_i32_field(4, 0); // CompressionCodec::UNCOMPRESSED
            fm.write_i64_field(5, num_rows); // num_values
            fm.write_i64_field(6, chunk_metas[i].total_size); // total_uncompressed_size
            fm.write_i64_field(7, chunk_metas[i].total_size); // total_compressed_size
            fm.write_i64_field(9, chunk_metas[i].data_page_offset); // data_page_offset
            fm.write_struct_end(); // ColumnMetaData
            fm.write_struct_end(); // ColumnChunk
        }
        fm.write_i64_field(2, rg_total_bytes); // total_byte_size
        fm.write_i64_field(3, num_rows); // num_rows
        fm.write_struct_end(); // RowGroup
    } else {
        fm.write_list_header(4, 12, 0);
    }

    fm.write_struct_end(); // FileMetaData

    std::fwrite(fm.buf.data(), 1, fm.buf.size(), f);
    uint32_t footer_len = static_cast<uint32_t>(fm.buf.size());
    std::fwrite(&footer_len, 4, 1, f);
    std::fwrite("PAR1", 1, 4, f);
    std::fclose(f);
}

} // namespace pq
