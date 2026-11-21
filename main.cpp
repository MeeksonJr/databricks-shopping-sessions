// MeeksonLabs Ultra-Fast Multithreaded C++20 Engine for Databricks Shopping Sessions
// Zero-copy mmap CSV scanner + 64-shard flat hash dedup + O(1) sliding-window attribution + parallel Parquet writer.

#include "parquet_writer.hpp"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace fs = std::filesystem;

// ============================================================================
// Fast 64-bit hash (wyhash-style mix)
// ============================================================================
static inline uint64_t hash_sv(std::string_view sv) noexcept {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(sv.data());
    size_t len = sv.size();
    uint64_t h = 0x9e3779b97f4a7c15ULL ^ (len * 0xbf58476d1ce4e5b9ULL);
    while (len >= 8) {
        uint64_t k;
        std::memcpy(&k, p, 8);
        k *= 0x94d049bb133111ebULL;
        k ^= k >> 31;
        h ^= k;
        h = (h * 0xbf58476d1ce4e5b9ULL) ^ (h >> 27);
        p += 8;
        len -= 8;
    }
    if (len > 0) {
        uint64_t k = 0;
        std::memcpy(&k, p, len);
        h ^= k * 0x94d049bb133111ebULL;
        h ^= h >> 31;
        h *= 0xbf58476d1ce4e5b9ULL;
    }
    h ^= h >> 30;
    h *= 0xbf58476d1ce4e5b9ULL;
    h ^= h >> 27;
    h *= 0x94d049bb133111ebULL;
    h ^= h >> 31;
    return h;
}

// ============================================================================
// RAII mmap wrapper (handles 0-byte files cleanly)
// ============================================================================
struct MmapFile {
    const char* data = nullptr;
    size_t size = 0;
    int fd = -1;

    MmapFile() = default;
    MmapFile(const MmapFile&) = delete;
    MmapFile& operator=(const MmapFile&) = delete;
    MmapFile(MmapFile&& o) noexcept : data(o.data), size(o.size), fd(o.fd) {
        o.data = nullptr; o.size = 0; o.fd = -1;
    }
    MmapFile& operator=(MmapFile&& o) noexcept {
        if (this != &o) {
            close_file();
            data = o.data; size = o.size; fd = o.fd;
            o.data = nullptr; o.size = 0; o.fd = -1;
        }
        return *this;
    }
    ~MmapFile() { close_file(); }

    void open_file(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("open failed: " + path);
        struct stat st;
        if (::fstat(fd, &st) != 0) {
            ::close(fd); fd = -1;
            throw std::runtime_error("fstat failed: " + path);
        }
        size = static_cast<size_t>(st.st_size);
        if (size > 0) {
            void* ptr = ::mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
            if (ptr == MAP_FAILED) {
                ::close(fd); fd = -1;
                throw std::runtime_error("mmap failed: " + path);
            }
            ::madvise(ptr, size, MADV_WILLNEED | MADV_SEQUENTIAL);
            data = static_cast<const char*>(ptr);
        }
        ::close(fd);
        fd = -1;
    }

    void close_file() {
        if (data && size > 0) ::munmap(const_cast<char*>(data), size);
        if (fd >= 0) ::close(fd);
        data = nullptr; size = 0; fd = -1;
    }
};

// ============================================================================
// Strict CSV line parser (rules.md exact dialect)
// Only \n ends a line; \r is ordinary; quotes only around whole field; no commas/quotes inside values.
// ============================================================================
template <int NCOLS>
static inline bool parse_csv_line(const char* line, size_t len, std::string_view out[NCOLS]) noexcept {
    if (len == 0) return false;
    const char* p = line;
    const char* end = line + len;
    int col = 0;
    const char* fstart = p;

    while (true) {
        // Find next comma or end
        const char* comma = static_cast<const char*>(std::memchr(p, ',', static_cast<size_t>(end - p)));
        const char* fend = comma ? comma : end;
        if (col >= NCOLS) return false;

        size_t flen = static_cast<size_t>(fend - fstart);
        if (flen >= 2 && fstart[0] == '"' && fstart[flen - 1] == '"') {
            const char* inner = fstart + 1;
            size_t ilen = flen - 2;
            if (ilen > 0 && std::memchr(inner, '"', ilen) != nullptr) return false;
            out[col] = std::string_view(inner, ilen);
        } else {
            if (flen > 0 && std::memchr(fstart, '"', flen) != nullptr) return false;
            out[col] = std::string_view(fstart, flen);
        }
        col++;

        if (!comma) break;
        p = comma + 1;
        fstart = p;
    }
    return col == NCOLS;
}

// ============================================================================
// Branchless Howard Hinnant days_from_civil & strict F1/F2/F3 timestamp parser
// ============================================================================
static inline bool is_ascii_digit(char c) noexcept {
    return static_cast<unsigned char>(c - '0') <= 9u;
}

static inline int64_t days_from_civil(int y, unsigned m, unsigned d) noexcept {
    y -= (m <= 2);
    const int era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(y - era * 400);
    const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return static_cast<int64_t>(era) * 146097 + static_cast<int64_t>(doe) - 719468;
}

static inline bool is_leap_year(int y) noexcept {
    return (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
}

static inline int days_in_month(int y, int m) noexcept {
    static constexpr int kDays[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (m == 2 && is_leap_year(y)) return 29;
    return kDays[m];
}

static inline bool parse_2d(const char* p, int& out) noexcept {
    unsigned d0 = static_cast<unsigned char>(p[0] - '0');
    unsigned d1 = static_cast<unsigned char>(p[1] - '0');
    if (d0 > 9u || d1 > 9u) return false;
    out = static_cast<int>(d0 * 10u + d1);
    return true;
}

static inline bool parse_4d(const char* p, int& out) noexcept {
    unsigned d0 = static_cast<unsigned char>(p[0] - '0');
    unsigned d1 = static_cast<unsigned char>(p[1] - '0');
    unsigned d2 = static_cast<unsigned char>(p[2] - '0');
    unsigned d3 = static_cast<unsigned char>(p[3] - '0');
    if (d0 > 9u || d1 > 9u || d2 > 9u || d3 > 9u) return false;
    out = static_cast<int>(d0 * 1000u + d1 * 100u + d2 * 10u + d3);
    return true;
}

static inline bool parse_ts_ms(std::string_view s, int64_t& out_ms) noexcept {
    size_t n = s.size();
    if (n == 0) return false;
    const char* p = s.data();

    // Check F1 (20 chars) or F2 (25 chars)
    if (n == 20 && p[4] == '-' && p[7] == '-' && p[10] == 'T' && p[13] == ':' && p[16] == ':' && p[19] == 'Z') {
        int y, mo, d, h, mi, sec;
        if (!parse_4d(p, y) || !parse_2d(p + 5, mo) || !parse_2d(p + 8, d) ||
            !parse_2d(p + 11, h) || !parse_2d(p + 14, mi) || !parse_2d(p + 17, sec))
            return false;
        if (mo < 1 || mo > 12 || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || sec > 59)
            return false;
        int64_t days = days_from_civil(y, static_cast<unsigned>(mo), static_cast<unsigned>(d));
        out_ms = (days * 86400LL + h * 3600LL + mi * 60LL + sec) * 1000LL;
        return true;
    }

    if (n == 25 && p[4] == '-' && p[7] == '-' && p[10] == 'T' && p[13] == ':' && p[16] == ':' &&
        (p[19] == '+' || p[19] == '-') && p[22] == ':') {
        int y, mo, d, h, mi, sec, oh, om;
        if (!parse_4d(p, y) || !parse_2d(p + 5, mo) || !parse_2d(p + 8, d) ||
            !parse_2d(p + 11, h) || !parse_2d(p + 14, mi) || !parse_2d(p + 17, sec) ||
            !parse_2d(p + 20, oh) || !parse_2d(p + 23, om))
            return false;
        if (mo < 1 || mo > 12 || d < 1 || d > days_in_month(y, mo) ||
            h > 23 || mi > 59 || sec > 59 || oh > 23 || om > 59)
            return false;
        int64_t days = days_from_civil(y, static_cast<unsigned>(mo), static_cast<unsigned>(d));
        int64_t utc_sec = days * 86400LL + h * 3600LL + mi * 60LL + sec;
        int64_t off_sec = oh * 3600LL + om * 60LL;
        if (p[19] == '+') utc_sec -= off_sec;
        else utc_sec += off_sec;
        out_ms = utc_sec * 1000LL;
        return true;
    }

    // Check F3: ASCII digits only (leading zeros allowed; huge numbers are valid format but outside window)
    size_t i = 0;
    while (i < n && p[i] == '0') i++;
    // Verify all remaining chars are ASCII digits
    for (size_t j = i; j < n; ++j) {
        if (!is_ascii_digit(p[j])) return false;
    }
    size_t rem = n - i;
    if (rem == 0) {
        out_ms = 0;
        return true;
    }
    if (rem > 18) {
        // Larger than 10^18 ms (~31M years) -> valid F3 syntax, guaranteed outside any window
        out_ms = std::numeric_limits<int64_t>::max();
        return true;
    }
    int64_t v = 0;
    for (size_t j = i; j < n; ++j) {
        v = v * 10 + (p[j] - '0');
    }
    out_ms = v;
    return true;
}

static inline bool parse_price_cents(std::string_view s, int32_t& out) noexcept {
    size_t n = s.size();
    if (n == 0) return false;
    const char* p = s.data();
    size_t i = 0;
    while (i < n && p[i] == '0') i++;
    if (i == n) return false; // value is 0 -> invalid (must be 1..10,000,000)
    if (n - i > 8) return false; // > 99,999,999 -> > 10,000,000 (check all digits? still false!)
    int64_t v = 0;
    for (; i < n; ++i) {
        if (!is_ascii_digit(p[i])) return false;
        v = v * 10 + (p[i] - '0');
    }
    if (v < 1 || v > 10000000) return false;
    out = static_cast<int32_t>(v);
    return true;
}

static inline int32_t floor_div_day(int64_t ms) noexcept {
    constexpr int64_t DAY_MS = 86400000LL;
    if (ms >= 0) return static_cast<int32_t>(ms / DAY_MS);
    return static_cast<int32_t>(-(((-ms) + DAY_MS - 1) / DAY_MS));
}

// ============================================================================
// Minimal JSON extractor for manifest.json ("window_start" & "window_end")
// ============================================================================
static std::string extract_json_str(const std::string& json, const std::string& key) {
    std::string pat = "\"" + key + "\"";
    auto pos = json.find(pat);
    if (pos == std::string::npos) throw std::runtime_error("Missing key in manifest: " + key);
    pos = json.find(':', pos + pat.size());
    auto q1 = json.find('"', pos + 1);
    auto q2 = json.find('"', q1 + 1);
    return json.substr(q1 + 1, q2 - q1 - 1);
}

// ============================================================================
// Fast flat open-addressing map: string_view -> uint32_t
// ============================================================================
struct FlatSVMap {
    struct Slot {
        uint64_t hash;
        const char* ptr;
        uint32_t len;
        uint32_t val;
    };
    std::vector<Slot> table;
    size_t mask = 0;
    size_t count = 0;

    void reserve(size_t expected) {
        size_t cap = 16;
        while (cap < expected * 2) cap <<= 1;
        table.assign(cap, Slot{0, nullptr, 0, 0});
        mask = cap - 1;
        count = 0;
    }

    void rehash(size_t new_cap) {
        std::vector<Slot> old = std::move(table);
        table.assign(new_cap, Slot{0, nullptr, 0, 0});
        mask = new_cap - 1;
        count = 0;
        for (const auto& s : old) {
            if (s.ptr != nullptr) {
                insert_unchecked(s.hash, s.ptr, s.len, s.val);
            }
        }
    }

    void insert_unchecked(uint64_t h, const char* ptr, uint32_t len, uint32_t val) {
        size_t idx = h & mask;
        while (table[idx].ptr != nullptr) idx = (idx + 1) & mask;
        table[idx] = Slot{h, ptr, len, val};
        count++;
    }

    // Returns {val, inserted_bool}
    std::pair<uint32_t, bool> get_or_insert(std::string_view sv, uint64_t h, uint32_t new_val) {
        if (table.empty()) reserve(64);
        if ((count + 1) * 10 >= table.size() * 7) rehash(table.size() * 2);
        size_t idx = h & mask;
        while (true) {
            auto& s = table[idx];
            if (s.ptr == nullptr) {
                s = Slot{h, sv.data(), static_cast<uint32_t>(sv.size()), new_val};
                count++;
                return {new_val, true};
            }
            if (s.hash == h && s.len == sv.size() && std::memcmp(s.ptr, sv.data(), s.len) == 0) {
                return {s.val, false};
            }
            idx = (idx + 1) & mask;
        }
    }

    inline bool find(std::string_view sv, uint64_t h, uint32_t& out_val) const noexcept {
        if (table.empty()) return false;
        size_t idx = h & mask;
        while (true) {
            const auto& s = table[idx];
            if (s.ptr == nullptr) return false;
            if (s.hash == h && s.len == sv.size() && std::memcmp(s.ptr, sv.data(), s.len) == 0) {
                out_val = s.val;
                return true;
            }
            idx = (idx + 1) & mask;
        }
    }
};

// ============================================================================
// Data structures (compact, zero-allocation per row!)
// ============================================================================
struct RawEvent {
    const char* eid_ptr;
    const char* uid_ptr;
    uint64_t eid_hash;
    int64_t ingest_ts;
    int64_t ts;
    uint32_t eid_len;
    uint32_t uid_len;
    int32_t price_cents;
    uint16_t cat_id;
    uint8_t etype; // 0 = invalid/poison, 1 = view, 2 = cart, 3 = purchase
};

struct CleanEvent {
    const char* uid_ptr;
    uint64_t uid_hash;
    int64_t ts;
    uint32_t uid_len;
    int32_t price_cents;
    uint16_t cat_id;
    uint8_t etype; // 1 = view, 2 = cart, 3 = purchase
};

struct RawClick {
    const char* cid_ptr;
    const char* uid_ptr;
    uint64_t cid_hash;
    int64_t ts;
    uint32_t cid_len;
    uint32_t uid_len;
    uint32_t camp_id;
};

struct CleanClick {
    const char* uid_ptr;
    const char* cid_ptr;
    uint64_t uid_hash;
    int64_t ts;
    uint32_t uid_len;
    uint32_t cid_len;
    uint32_t camp_id;
};

struct FileChunk {
    const char* start;
    const char* end;
};

struct CampDayAgg {
    int64_t sessions = 0;
    int64_t purchasing_sessions = 0;
    int64_t revenue_cents = 0;
    int64_t assisted_sessions = 0;
};

static std::vector<std::string> list_csvs(const fs::path& dir) {
    std::vector<std::string> files;
    if (!fs::exists(dir) || !fs::is_directory(dir)) return files;
    for (const auto& entry : fs::directory_iterator(dir)) {
        if (entry.is_regular_file() && entry.path().extension() == ".csv") {
            files.push_back(entry.path().string());
        }
    }
    std::sort(files.begin(), files.end());
    return files;
}

static void build_chunks(const std::vector<MmapFile>& files, std::vector<FileChunk>& chunks, size_t target_chunk = 2 << 20) {
    for (const auto& mf : files) {
        if (mf.size == 0) continue;
        const char* base = mf.data;
        const char* end = mf.data + mf.size;
        // Skip header line
        const char* nl = static_cast<const char*>(std::memchr(base, '\n', mf.size));
        if (!nl) continue;
        const char* pos = nl + 1;
        while (pos < end) {
            const char* cut = pos + target_chunk;
            if (cut >= end) {
                chunks.push_back({pos, end});
                break;
            }
            const char* next_nl = static_cast<const char*>(std::memchr(cut, '\n', static_cast<size_t>(end - cut)));
            if (!next_nl) {
                chunks.push_back({pos, end});
                break;
            }
            chunks.push_back({pos, next_nl + 1});
            pos = next_nl + 1;
        }
    }
}

static constexpr size_t NUM_BUCKETS = 64;
static constexpr int64_t LATE_MS = 24LL * 3600LL * 1000LL;
static constexpr int64_t SESSION_GAP_MS = 30LL * 60LL * 1000LL;
static constexpr int64_t SEVEN_DAYS_MS = 7LL * 24LL * 3600LL * 1000LL;

int main(int argc, char** argv) {
    std::string data_dir = "/data";
    std::string out_dir = "/output";
    if (const char* env_d = std::getenv("DATA_DIR")) data_dir = env_d;
    if (const char* env_o = std::getenv("OUTPUT_DIR")) out_dir = env_o;
    if (argc >= 3) {
        data_dir = argv[argc - 2];
        out_dir = argv[argc - 1];
    }

    int ncpu = static_cast<int>(std::thread::hardware_concurrency());
    if (const char* env_c = std::getenv("NCPU")) {
        int c = std::atoi(env_c);
        if (c > 0) ncpu = c;
    }
    if (ncpu < 1) ncpu = 1;
    if (ncpu > 16) ncpu = 16;

    // ------------------------------------------------------------------------
    // 1. Manifest
    // ------------------------------------------------------------------------
    int64_t win_start_ms = 0, win_end_ms = 0;
    {
        std::ifstream mf(fs::path(data_dir) / "manifest.json");
        std::string content((std::istreambuf_iterator<char>(mf)), std::istreambuf_iterator<char>());
        std::string ws = extract_json_str(content, "window_start");
        std::string we = extract_json_str(content, "window_end");
        if (!parse_ts_ms(ws, win_start_ms) || !parse_ts_ms(we, win_end_ms)) {
            throw std::runtime_error("Invalid manifest window timestamps");
        }
    }
    int64_t click_win_start_ms = win_start_ms - SEVEN_DAYS_MS;
    int32_t win_start_day = floor_div_day(win_start_ms);
    int32_t win_end_day = floor_div_day(win_end_ms - 1);
    int num_days = std::max(1, static_cast<int>(win_end_day - win_start_day + 1));

    // ------------------------------------------------------------------------
    // 2. Products (assign byte-wise sorted cat_ids so smaller cat_id wins ties!)
    // ------------------------------------------------------------------------
    MmapFile prod_file;
    std::vector<std::string> cat_names;
    FlatSVMap prod_to_cat;
    uint16_t unknown_cat_id = 0;
    {
        fs::path ppath = fs::path(data_dir) / "products.csv";
        std::vector<std::pair<std::string_view, std::string_view>> pairs;
        std::unordered_map<std::string_view, uint16_t> raw_cats;
        raw_cats.emplace("unknown", 0);
        if (fs::exists(ppath)) {
            prod_file.open_file(ppath.string());
            if (prod_file.size > 0) {
                const char* p = prod_file.data;
                const char* end = prod_file.data + prod_file.size;
                const char* nl = static_cast<const char*>(std::memchr(p, '\n', prod_file.size));
                if (nl) {
                    p = nl + 1;
                    std::string_view cols[2];
                    while (p < end) {
                        const char* eol = static_cast<const char*>(std::memchr(p, '\n', static_cast<size_t>(end - p)));
                        const char* lend = eol ? eol : end;
                        if (parse_csv_line<2>(p, static_cast<size_t>(lend - p), cols)) {
                            if (!cols[0].empty() && !cols[1].empty()) {
                                pairs.emplace_back(cols[0], cols[1]);
                                raw_cats.emplace(cols[1], 0);
                            }
                        }
                        p = eol ? (eol + 1) : end;
                    }
                }
            }
        }
        cat_names.reserve(raw_cats.size());
        for (const auto& kv : raw_cats) cat_names.emplace_back(kv.first);
        std::sort(cat_names.begin(), cat_names.end()); // Byte-wise lexicographical order!
        std::unordered_map<std::string_view, uint16_t> cat_id_map;
        cat_id_map.reserve(cat_names.size() * 2);
        for (size_t i = 0; i < cat_names.size(); ++i) {
            cat_id_map.emplace(cat_names[i], static_cast<uint16_t>(i));
        }
        unknown_cat_id = cat_id_map["unknown"];
        prod_to_cat.reserve(pairs.size() + 16);
        for (const auto& pr : pairs) {
            prod_to_cat.get_or_insert(pr.first, hash_sv(pr.first), cat_id_map[pr.second]);
        }
    }
    const size_t num_cats = cat_names.size();

    // ------------------------------------------------------------------------
    // 3. Campaigns (camp_id = 0 reserved for "organic")
    // ------------------------------------------------------------------------
    MmapFile camp_file;
    std::vector<std::string> camp_ids = {"organic"};
    std::vector<std::string> camp_channels = {"organic"};
    FlatSVMap camp_to_id;
    {
        fs::path cpath = fs::path(data_dir) / "campaigns.csv";
        if (fs::exists(cpath)) {
            camp_file.open_file(cpath.string());
            if (camp_file.size > 0) {
                const char* p = camp_file.data;
                const char* end = camp_file.data + camp_file.size;
                const char* nl = static_cast<const char*>(std::memchr(p, '\n', camp_file.size));
                if (nl) {
                    p = nl + 1;
                    std::string_view cols[2];
                    camp_to_id.reserve(4096);
                    while (p < end) {
                        const char* eol = static_cast<const char*>(std::memchr(p, '\n', static_cast<size_t>(end - p)));
                        const char* lend = eol ? eol : end;
                        if (parse_csv_line<2>(p, static_cast<size_t>(lend - p), cols)) {
                            if (!cols[0].empty() && !cols[1].empty()) {
                                uint32_t new_id = static_cast<uint32_t>(camp_ids.size());
                                auto [id, inserted] = camp_to_id.get_or_insert(cols[0], hash_sv(cols[0]), new_id);
                                if (inserted) {
                                    camp_ids.emplace_back(cols[0]);
                                    camp_channels.emplace_back(cols[1]);
                                }
                            }
                        }
                        p = eol ? (eol + 1) : end;
                    }
                }
            }
        }
    }
    const size_t num_camps = camp_ids.size();

    // ------------------------------------------------------------------------
    // 4. Links -> DSU -> Frozen Read-Only link_map (uid -> canonical uid)
    // ------------------------------------------------------------------------
    std::vector<MmapFile> link_files;
    FlatSVMap link_uid_to_idx;
    std::vector<std::string_view> link_uid_strs;
    std::vector<uint32_t> dsu_parent;
    std::vector<uint32_t> dsu_rank;
    std::vector<std::string_view> dsu_min_str;
    std::vector<uint64_t> dsu_min_hash;
    {
        auto lpaths = list_csvs(fs::path(data_dir) / "links");
        link_files.resize(lpaths.size());
        size_t total_bytes = 0;
        for (size_t i = 0; i < lpaths.size(); ++i) {
            link_files[i].open_file(lpaths[i]);
            total_bytes += link_files[i].size;
        }
        size_t est_nodes = std::max<size_t>(64, total_bytes / 16);
        link_uid_to_idx.reserve(est_nodes);
        link_uid_strs.reserve(est_nodes);
        dsu_parent.reserve(est_nodes);
        dsu_rank.reserve(est_nodes);
        dsu_min_str.reserve(est_nodes);

        auto dsu_find = [&](uint32_t x) {
            uint32_t r = x;
            while (dsu_parent[r] != r) r = dsu_parent[r];
            while (dsu_parent[x] != r) {
                uint32_t nxt = dsu_parent[x];
                dsu_parent[x] = r;
                x = nxt;
            }
            return r;
        };

        auto get_node = [&](std::string_view sv) -> uint32_t {
            uint32_t next_id = static_cast<uint32_t>(link_uid_strs.size());
            auto [id, ins] = link_uid_to_idx.get_or_insert(sv, hash_sv(sv), next_id);
            if (ins) {
                link_uid_strs.push_back(sv);
                dsu_parent.push_back(id);
                dsu_rank.push_back(0);
                dsu_min_str.push_back(sv);
            }
            return id;
        };

        std::string_view cols[2];
        for (const auto& mf : link_files) {
            if (mf.size == 0) continue;
            const char* p = mf.data;
            const char* end = mf.data + mf.size;
            const char* nl = static_cast<const char*>(std::memchr(p, '\n', mf.size));
            if (!nl) continue;
            p = nl + 1;
            while (p < end) {
                const char* eol = static_cast<const char*>(std::memchr(p, '\n', static_cast<size_t>(end - p)));
                const char* lend = eol ? eol : end;
                if (parse_csv_line<2>(p, static_cast<size_t>(lend - p), cols)) {
                    if (!cols[0].empty() && !cols[1].empty()) {
                        uint32_t a = get_node(cols[0]);
                        uint32_t b = get_node(cols[1]);
                        uint32_t ra = dsu_find(a);
                        uint32_t rb = dsu_find(b);
                        if (ra != rb) {
                            if (dsu_rank[ra] < dsu_rank[rb]) std::swap(ra, rb);
                            dsu_parent[rb] = ra;
                            if (dsu_rank[ra] == dsu_rank[rb]) dsu_rank[ra]++;
                            if (dsu_min_str[rb] < dsu_min_str[ra]) {
                                dsu_min_str[ra] = dsu_min_str[rb];
                            }
                        }
                    }
                }
                p = eol ? (eol + 1) : end;
            }
        }
        // Flatten all roots so dsu_min_str[i] is canonical for every node i
        size_t n = link_uid_strs.size();
        dsu_min_hash.resize(n);
        for (size_t i = 0; i < n; ++i) {
            uint32_t r = dsu_find(static_cast<uint32_t>(i));
            dsu_min_str[i] = dsu_min_str[r];
            dsu_min_hash[i] = hash_sv(dsu_min_str[i]);
        }
    }

    auto resolve_person = [&](std::string_view uid, uint64_t& out_hash) noexcept -> std::string_view {
        uint64_t h = hash_sv(uid);
        uint32_t idx;
        if (link_uid_to_idx.find(uid, h, idx)) {
            out_hash = dsu_min_hash[idx];
            return dsu_min_str[idx];
        }
        out_hash = h;
        return uid;
    };

    // ------------------------------------------------------------------------
    // 5. Events: Parallel mmap parse -> 64-bucket dedup -> 64-person-shard route
    // ------------------------------------------------------------------------
    std::vector<MmapFile> ev_files;
    std::vector<FileChunk> ev_chunks;
    {
        auto epaths = list_csvs(fs::path(data_dir) / "events");
        ev_files.resize(epaths.size());
        for (size_t i = 0; i < epaths.size(); ++i) ev_files[i].open_file(epaths[i]);
        build_chunks(ev_files, ev_chunks);
    }

    size_t total_ev_bytes = 0;
    for (const auto& mf : ev_files) total_ev_bytes += mf.size;
    size_t est_total_evs = total_ev_bytes / 55;
    size_t per_bucket_ev_cap = est_total_evs / (ncpu * NUM_BUCKETS) + 4096;

    // Per-thread raw event buckets [tid][NUM_BUCKETS]
    std::vector<std::vector<std::vector<RawEvent>>> raw_ev_buckets(
        ncpu, std::vector<std::vector<RawEvent>>(NUM_BUCKETS));

    {
        std::atomic<size_t> next_chunk{0};
        std::vector<std::thread> workers;
        for (int t = 0; t < ncpu; ++t) {
            workers.emplace_back([&, t]() {
                auto& my_buckets = raw_ev_buckets[t];
                for (size_t b = 0; b < NUM_BUCKETS; ++b) {
                    my_buckets[b].reserve(per_bucket_ev_cap);
                }
                std::string_view cols[7];
                while (true) {
                    size_t ci = next_chunk.fetch_add(1, std::memory_order_relaxed);
                    if (ci >= ev_chunks.size()) break;
                    const char* p = ev_chunks[ci].start;
                    const char* end = ev_chunks[ci].end;
                    while (p < end) {
                        const char* eol = static_cast<const char*>(std::memchr(p, '\n', static_cast<size_t>(end - p)));
                        const char* lend = eol ? eol : end;
                        if (parse_csv_line<7>(p, static_cast<size_t>(lend - p), cols)) {
                            std::string_view eid = cols[0];
                            if (!eid.empty()) {
                                int64_t ingest_ts = 0;
                                if (parse_ts_ms(cols[3], ingest_ts)) {
                                    uint64_t eh = hash_sv(eid);
                                    uint8_t etype = 0;
                                    int64_t ts = 0;
                                    int32_t price = 0;
                                    uint16_t cat_id = unknown_cat_id;

                                    std::string_view uid = cols[1];
                                    std::string_view pid = cols[5];
                                    if (!uid.empty() && !pid.empty() && parse_ts_ms(cols[2], ts) &&
                                        ts >= win_start_ms && ts < win_end_ms &&
                                        (ingest_ts - ts) <= LATE_MS) {
                                        std::string_view et = cols[4];
                                        if (et == "view") {
                                            etype = 1;
                                        } else if (et == "cart") {
                                            etype = 2;
                                        } else if (et == "purchase") {
                                            if (parse_price_cents(cols[6], price)) {
                                                etype = 3;
                                                uint32_t cid;
                                                if (prod_to_cat.find(pid, hash_sv(pid), cid)) {
                                                    cat_id = static_cast<uint16_t>(cid);
                                                }
                                            }
                                        }
                                    }

                                    my_buckets[eh & (NUM_BUCKETS - 1)].push_back(RawEvent{
                                        eid.data(), uid.data(), eh, ingest_ts, ts,
                                        static_cast<uint32_t>(eid.size()),
                                        static_cast<uint32_t>(uid.size()),
                                        price, cat_id, etype
                                    });
                                }
                            }
                        }
                        p = eol ? (eol + 1) : end;
                    }
                }
            });
        }
        for (auto& w : workers) w.join();
    }

    // Phase 5b: Parallel event dedup per bucket -> route CleanEvents into 64 person shards
    std::vector<std::vector<std::vector<CleanEvent>>> person_ev_shards(
        ncpu, std::vector<std::vector<CleanEvent>>(NUM_BUCKETS));

    {
        std::atomic<size_t> next_b{0};
        std::vector<std::thread> workers;
        for (int t = 0; t < ncpu; ++t) {
            workers.emplace_back([&, t]() {
                auto& my_out = person_ev_shards[t];
                struct DedupSlot {
                    uint64_t hash;
                    const char* ptr;
                    uint32_t len;
                    uint32_t best_idx;
                };
                std::vector<RawEvent> merged;
                std::vector<DedupSlot> dtab;

                while (true) {
                    size_t b = next_b.fetch_add(1, std::memory_order_relaxed);
                    if (b >= NUM_BUCKETS) break;

                    size_t total_n = 0;
                    for (int s = 0; s < ncpu; ++s) total_n += raw_ev_buckets[s][b].size();
                    if (total_n == 0) continue;

                    merged.clear();
                    merged.reserve(total_n);
                    for (int s = 0; s < ncpu; ++s) {
                        auto& vec = raw_ev_buckets[s][b];
                        merged.insert(merged.end(), vec.begin(), vec.end());
                        std::vector<RawEvent>().swap(vec); // free immediately!
                    }

                    size_t cap = 16;
                    while (cap < total_n * 2) cap <<= 1;
                    dtab.assign(cap, DedupSlot{0, nullptr, 0, 0});
                    size_t mask = cap - 1;

                    for (uint32_t i = 0; i < merged.size(); ++i) {
                        const auto& e = merged[i];
                        size_t idx = (e.eid_hash >> 6) & mask;
                        while (true) {
                            auto& sl = dtab[idx];
                            if (sl.ptr == nullptr) {
                                sl = DedupSlot{e.eid_hash, e.eid_ptr, e.eid_len, i};
                                break;
                            }
                            if (sl.hash == e.eid_hash && sl.len == e.eid_len &&
                                std::memcmp(sl.ptr, e.eid_ptr, sl.len) == 0) {
                                if (e.ingest_ts < merged[sl.best_idx].ingest_ts) {
                                    sl.best_idx = i;
                                }
                                break;
                            }
                            idx = (idx + 1) & mask;
                        }
                    }

                    for (const auto& sl : dtab) {
                        if (sl.ptr == nullptr) continue;
                        const auto& e = merged[sl.best_idx];
                        if (e.etype == 0) continue; // Invalid kept copy -> dropped!
                        uint64_t ph = 0;
                        std::string_view puid = resolve_person(std::string_view(e.uid_ptr, e.uid_len), ph);
                        my_out[ph & (NUM_BUCKETS - 1)].push_back(CleanEvent{
                            puid.data(), ph, e.ts, static_cast<uint32_t>(puid.size()),
                            e.price_cents, e.cat_id, e.etype
                        });
                    }
                }
            });
        }
        for (auto& w : workers) w.join();
    }
    raw_ev_buckets.clear();
    raw_ev_buckets.shrink_to_fit();

    // ------------------------------------------------------------------------
    // 6. Clicks: Parallel mmap parse -> 64-bucket dedup -> 64-person-shard route
    // ------------------------------------------------------------------------
    std::vector<MmapFile> clk_files;
    std::vector<FileChunk> clk_chunks;
    {
        auto cpaths = list_csvs(fs::path(data_dir) / "clicks");
        clk_files.resize(cpaths.size());
        for (size_t i = 0; i < cpaths.size(); ++i) clk_files[i].open_file(cpaths[i]);
        build_chunks(clk_files, clk_chunks);
    }

    size_t total_clk_bytes = 0;
    for (const auto& mf : clk_files) total_clk_bytes += mf.size;
    size_t est_total_clks = total_clk_bytes / 45;
    size_t per_bucket_clk_cap = est_total_clks / (ncpu * NUM_BUCKETS) + 2048;

    std::vector<std::vector<std::vector<RawClick>>> raw_clk_buckets(
        ncpu, std::vector<std::vector<RawClick>>(NUM_BUCKETS));

    {
        std::atomic<size_t> next_chunk{0};
        std::vector<std::thread> workers;
        for (int t = 0; t < ncpu; ++t) {
            workers.emplace_back([&, t]() {
                auto& my_buckets = raw_clk_buckets[t];
                for (size_t b = 0; b < NUM_BUCKETS; ++b) {
                    my_buckets[b].reserve(per_bucket_clk_cap);
                }
                std::string_view cols[4];
                while (true) {
                    size_t ci = next_chunk.fetch_add(1, std::memory_order_relaxed);
                    if (ci >= clk_chunks.size()) break;
                    const char* p = clk_chunks[ci].start;
                    const char* end = clk_chunks[ci].end;
                    while (p < end) {
                        const char* eol = static_cast<const char*>(std::memchr(p, '\n', static_cast<size_t>(end - p)));
                        const char* lend = eol ? eol : end;
                        if (parse_csv_line<4>(p, static_cast<size_t>(lend - p), cols)) {
                            std::string_view cid = cols[0];
                            std::string_view uid = cols[1];
                            std::string_view camp = cols[2];
                            int64_t ts = 0;
                            uint32_t camp_id = 0;
                            if (!cid.empty() && !uid.empty() && !camp.empty() &&
                                camp_to_id.find(camp, hash_sv(camp), camp_id) &&
                                parse_ts_ms(cols[3], ts) &&
                                ts >= click_win_start_ms && ts < win_end_ms) {
                                uint64_t ch = hash_sv(cid);
                                my_buckets[ch & (NUM_BUCKETS - 1)].push_back(RawClick{
                                    cid.data(), uid.data(), ch, ts,
                                    static_cast<uint32_t>(cid.size()),
                                    static_cast<uint32_t>(uid.size()),
                                    camp_id
                                });
                            }
                        }
                        p = eol ? (eol + 1) : end;
                    }
                }
            });
        }
        for (auto& w : workers) w.join();
    }

    std::vector<std::vector<std::vector<CleanClick>>> person_clk_shards(
        ncpu, std::vector<std::vector<CleanClick>>(NUM_BUCKETS));

    {
        std::atomic<size_t> next_b{0};
        std::vector<std::thread> workers;
        for (int t = 0; t < ncpu; ++t) {
            workers.emplace_back([&, t]() {
                auto& my_out = person_clk_shards[t];
                struct ClkSlot {
                    uint64_t hash;
                    const char* ptr;
                    uint32_t len;
                };
                std::vector<ClkSlot> ctab;

                while (true) {
                    size_t b = next_b.fetch_add(1, std::memory_order_relaxed);
                    if (b >= NUM_BUCKETS) break;

                    size_t total_n = 0;
                    for (int s = 0; s < ncpu; ++s) total_n += raw_clk_buckets[s][b].size();
                    if (total_n == 0) continue;

                    size_t cap = 16;
                    while (cap < total_n * 2) cap <<= 1;
                    ctab.assign(cap, ClkSlot{0, nullptr, 0});
                    size_t mask = cap - 1;

                    for (int s = 0; s < ncpu; ++s) {
                        auto& vec = raw_clk_buckets[s][b];
                        for (const auto& c : vec) {
                            size_t idx = (c.cid_hash >> 6) & mask;
                            bool dup = false;
                            while (true) {
                                auto& sl = ctab[idx];
                                if (sl.ptr == nullptr) {
                                    sl = ClkSlot{c.cid_hash, c.cid_ptr, c.cid_len};
                                    break;
                                }
                                if (sl.hash == c.cid_hash && sl.len == c.cid_len &&
                                    std::memcmp(sl.ptr, c.cid_ptr, sl.len) == 0) {
                                    dup = true;
                                    break;
                                }
                                idx = (idx + 1) & mask;
                            }
                            if (!dup) {
                                uint64_t ph = 0;
                                std::string_view puid = resolve_person(std::string_view(c.uid_ptr, c.uid_len), ph);
                                my_out[ph & (NUM_BUCKETS - 1)].push_back(CleanClick{
                                    puid.data(), c.cid_ptr, ph, c.ts,
                                    static_cast<uint32_t>(puid.size()), c.cid_len, c.camp_id
                                });
                            }
                        }
                        std::vector<RawClick>().swap(vec);
                    }
                }
            });
        }
        for (auto& w : workers) w.join();
    }
    raw_clk_buckets.clear();
    raw_clk_buckets.shrink_to_fit();

    // ------------------------------------------------------------------------
    // 7. Parallel Sessionize + Attribute + Write user_daily/part-<t>.parquet
    // ------------------------------------------------------------------------
    fs::create_directories(fs::path(out_dir) / "user_daily");
    fs::create_directories(fs::path(out_dir) / "campaign_daily");

    std::vector<std::vector<CampDayAgg>> thread_camp_daily(
        ncpu, std::vector<CampDayAgg>(num_camps * num_days));

    {
        std::atomic<size_t> next_p{0};
        std::vector<std::thread> workers;
        for (int t = 0; t < ncpu; ++t) {
            workers.emplace_back([&, t]() {
                std::vector<pq::ColumnBuffer> ud_cols = {
                    {{"user_id", pq::ColKind::UTF8_REQ}},
                    {{"day", pq::ColKind::DATE32}},
                    {{"sessions", pq::ColKind::INT64}},
                    {{"events", pq::ColKind::INT64}},
                    {{"revenue_cents", pq::ColKind::INT64}},
                    {{"top_category", pq::ColKind::UTF8_OPT}},
                };
                auto& my_camp = thread_camp_daily[t];

                // Reusable per-thread scratch structures
                FlatSVMap pmap;
                std::vector<std::string_view> p_uids;
                std::vector<uint32_t> ev_counts, ev_offsets, clk_counts, clk_offsets;
                std::vector<CleanEvent> sorted_evs;
                std::vector<CleanClick> sorted_clks;

                // Per-person day accumulators
                struct DayAcc {
                    int64_t sessions = 0;
                    int64_t events = 0;
                    int64_t revenue = 0;
                    bool has_purchase = false;
                };
                std::vector<DayAcc> day_acc(num_days);
                std::vector<int> active_days;
                // Per-(day, cat) purchase revenue (sparse reset!)
                std::vector<int64_t> day_cat_rev(num_days * num_cats, 0);
                std::vector<std::vector<uint16_t>> day_active_cats(num_days);

                // Assist deduplication per session (monotonically increasing sequence)
                std::vector<uint64_t> camp_last_sess(num_camps, 0);
                uint64_t sess_seq = 0;

                while (true) {
                    size_t p = next_p.fetch_add(1, std::memory_order_relaxed);
                    if (p >= NUM_BUCKETS) break;

                    size_t total_evs = 0;
                    for (int s = 0; s < ncpu; ++s) total_evs += person_ev_shards[s][p].size();
                    if (total_evs == 0) {
                        for (int s = 0; s < ncpu; ++s) std::vector<CleanClick>().swap(person_clk_shards[s][p]);
                        continue;
                    }

                    // Build person index for this shard
                    pmap.reserve(std::max<size_t>(64, total_evs / 4));
                    p_uids.clear();
                    ev_counts.clear();

                    std::vector<uint32_t> ev_owner;
                    ev_owner.reserve(total_evs);
                    for (int s = 0; s < ncpu; ++s) {
                        const auto& vec = person_ev_shards[s][p];
                        for (const auto& e : vec) {
                            uint32_t nid = static_cast<uint32_t>(p_uids.size());
                            auto [pid, ins] = pmap.get_or_insert(std::string_view(e.uid_ptr, e.uid_len), e.uid_hash, nid);
                            if (ins) {
                                p_uids.emplace_back(e.uid_ptr, e.uid_len);
                                ev_counts.push_back(0);
                            }
                            ev_counts[pid]++;
                            ev_owner.push_back(pid);
                        }
                    }

                    size_t n_persons = p_uids.size();
                    ev_offsets.assign(n_persons + 1, 0);
                    for (size_t i = 0; i < n_persons; ++i) ev_offsets[i + 1] = ev_offsets[i] + ev_counts[i];
                    sorted_evs.resize(total_evs);
                    std::vector<uint32_t> cursor = ev_offsets;
                    size_t ei = 0;
                    for (int s = 0; s < ncpu; ++s) {
                        auto& vec = person_ev_shards[s][p];
                        for (const auto& e : vec) {
                            uint32_t pid = ev_owner[ei++];
                            sorted_evs[cursor[pid]++] = e;
                        }
                        std::vector<CleanEvent>().swap(vec);
                    }

                    // Group clicks only for persons present in pmap!
                    size_t total_clks = 0;
                    clk_counts.assign(n_persons, 0);
                    std::vector<std::pair<uint32_t, CleanClick>> matched_clks;
                    for (int s = 0; s < ncpu; ++s) total_clks += person_clk_shards[s][p].size();
                    matched_clks.reserve(total_clks);
                    for (int s = 0; s < ncpu; ++s) {
                        auto& vec = person_clk_shards[s][p];
                        for (const auto& c : vec) {
                            uint32_t pid;
                            if (pmap.find(std::string_view(c.uid_ptr, c.uid_len), c.uid_hash, pid)) {
                                clk_counts[pid]++;
                                matched_clks.emplace_back(pid, c);
                            }
                        }
                        std::vector<CleanClick>().swap(vec);
                    }
                    clk_offsets.assign(n_persons + 1, 0);
                    for (size_t i = 0; i < n_persons; ++i) clk_offsets[i + 1] = clk_offsets[i] + clk_counts[i];
                    sorted_clks.resize(matched_clks.size());
                    cursor = clk_offsets;
                    for (const auto& pr : matched_clks) {
                        sorted_clks[cursor[pr.first]++] = pr.second;
                    }
                    matched_clks.clear();

                    // Process each person
                    for (size_t pid = 0; pid < n_persons; ++pid) {
                        uint32_t eb = ev_offsets[pid], ee = ev_offsets[pid + 1];
                        std::sort(sorted_evs.begin() + eb, sorted_evs.begin() + ee,
                                  [](const CleanEvent& a, const CleanEvent& b) { return a.ts < b.ts; });

                        uint32_t cb = clk_offsets[pid], ce = clk_offsets[pid + 1];
                        if (ce - cb > 1) {
                            // Sort clicks by (ts ASC, click_id DESC) so clks[R-1] is ALWAYS max ts, min click_id!
                            std::sort(sorted_clks.begin() + cb, sorted_clks.begin() + ce,
                                      [](const CleanClick& a, const CleanClick& b) {
                                          if (a.ts != b.ts) return a.ts < b.ts;
                                          return std::string_view(a.cid_ptr, a.cid_len) >
                                                 std::string_view(b.cid_ptr, b.cid_len);
                                      });
                        }

                        uint32_t L = cb, R = cb;
                        active_days.clear();

                        uint32_t i = eb;
                        while (i < ee) {
                            uint32_t s_start_idx = i;
                            int64_t sess_start = sorted_evs[i].ts;
                            int64_t sess_rev = 0;
                            bool sess_purch = false;
                            int32_t sess_day = floor_div_day(sess_start);
                            int d_idx = static_cast<int>(sess_day - win_start_day);

                            if (day_acc[d_idx].sessions == 0) active_days.push_back(d_idx);

                            // Advance session events
                            while (true) {
                                const auto& ev = sorted_evs[i];
                                if (ev.etype == 3) {
                                    sess_purch = true;
                                    sess_rev += ev.price_cents;
                                    size_t dci = static_cast<size_t>(d_idx) * num_cats + ev.cat_id;
                                    if (day_cat_rev[dci] == 0) day_active_cats[d_idx].push_back(ev.cat_id);
                                    day_cat_rev[dci] += ev.price_cents;
                                }
                                i++;
                                if (i >= ee || (sorted_evs[i].ts - sorted_evs[i - 1].ts) > SESSION_GAP_MS) break;
                            }
                            int64_t sess_evs = static_cast<int64_t>(i - s_start_idx);

                            day_acc[d_idx].sessions += 1;
                            day_acc[d_idx].events += sess_evs;
                            day_acc[d_idx].revenue += sess_rev;
                            if (sess_purch) day_acc[d_idx].has_purchase = true;

                            // Sliding click window [L, R)
                            while (R < ce && sorted_clks[R].ts <= sess_start) {
                                R++;
                            }
                            while (L < R && (sess_start - sorted_clks[L].ts) > SEVEN_DAYS_MS) {
                                L++;
                            }

                            if (L < R) {
                                uint32_t credited = sorted_clks[R - 1].camp_id;
                                auto& cd = my_camp[static_cast<size_t>(credited) * num_days + d_idx];
                                cd.sessions += 1;
                                if (sess_purch) cd.purchasing_sessions += 1;
                                cd.revenue_cents += sess_rev;

                                // Assists: each distinct qualifying campaign assists this session once
                                sess_seq++;
                                for (uint32_t k = L; k < R; ++k) {
                                    uint32_t ac = sorted_clks[k].camp_id;
                                    if (camp_last_sess[ac] != sess_seq) {
                                        camp_last_sess[ac] = sess_seq;
                                        my_camp[static_cast<size_t>(ac) * num_days + d_idx].assisted_sessions += 1;
                                    }
                                }
                            } else {
                                // Organic (camp_id = 0)
                                auto& cd = my_camp[static_cast<size_t>(0) * num_days + d_idx];
                                cd.sessions += 1;
                                if (sess_purch) cd.purchasing_sessions += 1;
                                cd.revenue_cents += sess_rev;
                            }
                        }

                        // Emit user_daily rows for this person & reset day accumulators
                        std::string_view puid = p_uids[pid];
                        for (int d_idx : active_days) {
                            const auto& da = day_acc[d_idx];
                            ud_cols[0].append_str(puid);
                            ud_cols[1].append_i32(win_start_day + d_idx);
                            ud_cols[2].append_i64(da.sessions);
                            ud_cols[3].append_i64(da.events);
                            ud_cols[4].append_i64(da.revenue);

                            if (da.has_purchase) {
                                int64_t best_rev = -1;
                                uint16_t best_cat = 0;
                                for (uint16_t cid : day_active_cats[d_idx]) {
                                    size_t dci = static_cast<size_t>(d_idx) * num_cats + cid;
                                    int64_t r = day_cat_rev[dci];
                                    if (r > best_rev || (r == best_rev && cid < best_cat)) {
                                        best_rev = r;
                                        best_cat = cid;
                                    }
                                    day_cat_rev[dci] = 0;
                                }
                                day_active_cats[d_idx].clear();
                                ud_cols[5].append_opt_str(cat_names[best_cat], true);
                            } else {
                                ud_cols[5].append_opt_str("", false);
                            }
                            day_acc[d_idx] = DayAcc{};
                        }
                    }
                }

                // Thread 0 ALWAYS writes part-0.parquet (even if empty); others write if non-empty
                if (t == 0 || ud_cols[0].num_rows > 0) {
                    std::string out_path = (fs::path(out_dir) / "user_daily" / ("part-" + std::to_string(t) + ".parquet")).string();
                    pq::write_parquet_file(out_path, ud_cols);
                }
            });
        }
        for (auto& w : workers) w.join();
    }

    // ------------------------------------------------------------------------
    // 8. Merge tiny campaign_daily arrays & write campaign_daily/part-0.parquet
    // ------------------------------------------------------------------------
    std::vector<pq::ColumnBuffer> cd_cols = {
        {{"campaign_id", pq::ColKind::UTF8_REQ}},
        {{"channel", pq::ColKind::UTF8_REQ}},
        {{"day", pq::ColKind::DATE32}},
        {{"sessions", pq::ColKind::INT64}},
        {{"purchasing_sessions", pq::ColKind::INT64}},
        {{"revenue_cents", pq::ColKind::INT64}},
        {{"assisted_sessions", pq::ColKind::INT64}},
    };

    for (size_t c = 0; c < num_camps; ++c) {
        for (int d = 0; d < num_days; ++d) {
            size_t idx = c * num_days + d;
            int64_t s = 0, ps = 0, rev = 0, ast = 0;
            for (int t = 0; t < ncpu; ++t) {
                const auto& a = thread_camp_daily[t][idx];
                s += a.sessions;
                ps += a.purchasing_sessions;
                rev += a.revenue_cents;
                ast += a.assisted_sessions;
            }
            if (s > 0 || ast > 0) {
                cd_cols[0].append_str(camp_ids[c]);
                cd_cols[1].append_str(camp_channels[c]);
                cd_cols[2].append_i32(win_start_day + d);
                cd_cols[3].append_i64(s);
                cd_cols[4].append_i64(ps);
                cd_cols[5].append_i64(rev);
                cd_cols[6].append_i64(ast);
            }
        }
    }

    pq::write_parquet_file(
        (fs::path(out_dir) / "campaign_daily" / "part-0.parquet").string(),
        cd_cols
    );

    return 0;
}
