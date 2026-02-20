#include <cstdint>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <cstdlib>

#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"

namespace {

bool DebugEnabled() {
  const char* v = std::getenv("KVTOOL_DEBUG");
  return v != nullptr && *v != '\0' && std::string(v) != "0";
}

void Debug(const std::string& msg) {
  if (DebugEnabled()) {
    std::cerr << "[kvtool] " << msg << "\n";
  }
}

struct Config {
  std::string cmd;
  std::string db_path;
  std::string workload_path;
  std::string queries_path;
  std::string out_path;
  std::string compression = "snappy";
  int reopen_every = 0;
  int bloom_bits = 10;
  int crash_after = 0;
};

void Usage() {
  std::cerr
      << "Usage:\n"
      << "  leveldb_kvtool apply --db <path> --workload <file> [--reopen-every N] [--bloom-bits N] [--compression none|snappy]\n"
      << "  leveldb_kvtool apply-crash --db <path> --workload <file> --crash-after N [--bloom-bits N] [--compression none|snappy]\n"
      << "  leveldb_kvtool transcript --db <path> --queries <file> --out <file> [--bloom-bits N]\n";
}

bool ParseInt(const std::string& s, int* out) {
  try {
    size_t idx = 0;
    int v = std::stoi(s, &idx);
    if (idx != s.size()) {
      return false;
    }
    *out = v;
    return true;
  } catch (...) {
    return false;
  }
}

bool ParseArgs(int argc, char** argv, Config* cfg) {
  if (argc < 2) {
    return false;
  }

  cfg->cmd = argv[1];

  std::unordered_map<std::string, std::string> kv;
  for (int i = 2; i < argc; i++) {
    std::string k = argv[i];
    if (k.rfind("--", 0) != 0) {
      std::cerr << "invalid arg: " << k << "\n";
      return false;
    }
    if (i + 1 >= argc) {
      std::cerr << "missing value for arg: " << k << "\n";
      return false;
    }
    kv[k] = argv[++i];
  }

  auto get = [&](const std::string& key, std::string* out) -> bool {
    auto it = kv.find(key);
    if (it == kv.end()) return false;
    *out = it->second;
    return true;
  };

  auto getInt = [&](const std::string& key, int* out) -> bool {
    auto it = kv.find(key);
    if (it == kv.end()) return false;
    return ParseInt(it->second, out);
  };

  if (!get("--db", &cfg->db_path)) {
    std::cerr << "--db is required\n";
    return false;
  }

  if (cfg->cmd == "apply" || cfg->cmd == "apply-crash") {
    if (!get("--workload", &cfg->workload_path)) {
      std::cerr << "--workload is required\n";
      return false;
    }
    if (cfg->cmd == "apply-crash") {
      if (!getInt("--crash-after", &cfg->crash_after) || cfg->crash_after <= 0) {
        std::cerr << "--crash-after must be > 0 for apply-crash\n";
        return false;
      }
    }
  } else if (cfg->cmd == "transcript") {
    if (!get("--queries", &cfg->queries_path)) {
      std::cerr << "--queries is required\n";
      return false;
    }
    if (!get("--out", &cfg->out_path)) {
      std::cerr << "--out is required\n";
      return false;
    }
  } else {
    std::cerr << "unknown command: " << cfg->cmd << "\n";
    return false;
  }

  if (kv.count("--reopen-every") > 0 && !getInt("--reopen-every", &cfg->reopen_every)) {
    std::cerr << "invalid --reopen-every\n";
    return false;
  }
  if (kv.count("--bloom-bits") > 0 && !getInt("--bloom-bits", &cfg->bloom_bits)) {
    std::cerr << "invalid --bloom-bits\n";
    return false;
  }
  if (kv.count("--compression") > 0 && !get("--compression", &cfg->compression)) {
    std::cerr << "invalid --compression\n";
    return false;
  }
  if (cfg->compression != "none" && cfg->compression != "snappy") {
    std::cerr << "unsupported --compression: " << cfg->compression << "\n";
    return false;
  }

  return true;
}

bool DecodeHex(const std::string& in, std::string* out) {
  if (in == "-") {
    out->clear();
    return true;
  }
  if (in.size() % 2 != 0) {
    return false;
  }

  auto hexVal = [](char c) -> int {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
  };

  out->clear();
  out->reserve(in.size() / 2);
  for (size_t i = 0; i < in.size(); i += 2) {
    int hi = hexVal(in[i]);
    int lo = hexVal(in[i + 1]);
    if (hi < 0 || lo < 0) {
      return false;
    }
    out->push_back(static_cast<char>((hi << 4) | lo));
  }
  return true;
}

bool ReadOpLine(const std::string& line, char* op, std::string* a, std::string* b) {
  if (line.empty() || line[0] == '#') {
    return false;
  }

  std::vector<std::string> parts;
  std::string cur;
  for (char c : line) {
    if (c == ' ' || c == '\t') {
      if (!cur.empty()) {
        parts.push_back(cur);
        cur.clear();
      }
      continue;
    }
    cur.push_back(c);
  }
  if (!cur.empty()) {
    parts.push_back(cur);
  }
  if (parts.empty()) {
    return false;
  }

  *op = parts[0][0];
  if (*op == 'P') {
    if (parts.size() != 3) {
      throw std::runtime_error("invalid P line: " + line);
    }
    *a = parts[1];
    *b = parts[2];
    return true;
  }
  if (*op == 'D' || *op == 'G' || *op == 'S') {
    if (parts.size() != 2) {
      throw std::runtime_error("invalid line: " + line);
    }
    *a = parts[1];
    b->clear();
    return true;
  }

  throw std::runtime_error("unknown op: " + line);
}

void WriteU64(std::ofstream* out, std::uint64_t v) {
  char buf[8];
  for (int i = 0; i < 8; i++) {
    buf[i] = static_cast<char>((v >> (8 * i)) & 0xff);
  }
  out->write(buf, sizeof(buf));
}

void WriteBytes(std::ofstream* out, const leveldb::Slice& s) {
  WriteU64(out, static_cast<std::uint64_t>(s.size()));
  if (!s.empty()) {
    out->write(s.data(), static_cast<std::streamsize>(s.size()));
  }
}

void WriteBytes(std::ofstream* out, const std::string& s) {
  WriteU64(out, static_cast<std::uint64_t>(s.size()));
  if (!s.empty()) {
    out->write(s.data(), static_cast<std::streamsize>(s.size()));
  }
}

void MustOK(const leveldb::Status& s, const std::string& ctx) {
  if (!s.ok()) {
    throw std::runtime_error(ctx + ": " + s.ToString());
  }
}

std::unique_ptr<leveldb::DB> OpenDB(const std::string& path, bool create_if_missing, int bloom_bits,
                                    const std::string& compression,
                                    std::unique_ptr<const leveldb::FilterPolicy>* out_policy) {
  leveldb::Options options;
  options.create_if_missing = create_if_missing;
  options.error_if_exists = false;
  options.compression = (compression == "none") ? leveldb::kNoCompression : leveldb::kSnappyCompression;
  if (bloom_bits > 0) {
    options.filter_policy = leveldb::NewBloomFilterPolicy(bloom_bits);
    out_policy->reset(options.filter_policy);
  } else {
    options.filter_policy = nullptr;
    out_policy->reset();
  }

  leveldb::DB* raw = nullptr;
  leveldb::Status s = leveldb::DB::Open(options, path, &raw);
  if (!s.ok()) {
    throw std::runtime_error("open failed: " + s.ToString());
  }
  return std::unique_ptr<leveldb::DB>(raw);
}

int RunApply(const Config& cfg, bool crash_mode) {
  std::ifstream in(cfg.workload_path);
  if (!in.is_open()) {
    std::cerr << "failed to open workload: " << cfg.workload_path << "\n";
    return 1;
  }

  std::unique_ptr<const leveldb::FilterPolicy> policy;
  std::unique_ptr<leveldb::DB> db = OpenDB(cfg.db_path, true, cfg.bloom_bits, cfg.compression, &policy);

  leveldb::WriteOptions wo;
  std::size_t n = 0;

  std::string line;
  while (std::getline(in, line)) {
    char op;
    std::string tokA;
    std::string tokB;
    bool ok = false;
    try {
      ok = ReadOpLine(line, &op, &tokA, &tokB);
    } catch (const std::exception& ex) {
      std::cerr << ex.what() << "\n";
      return 1;
    }
    if (!ok) continue;

    std::string key;
    if (!DecodeHex(tokA, &key)) {
      std::cerr << "bad hex key: " << tokA << "\n";
      return 1;
    }

    if (op == 'P') {
      std::string value;
      if (!DecodeHex(tokB, &value)) {
        std::cerr << "bad hex value\n";
        return 1;
      }
      MustOK(db->Put(wo, key, value), "put");
    } else if (op == 'D') {
      MustOK(db->Delete(wo, key), "delete");
    } else {
      std::cerr << "invalid workload op: " << op << "\n";
      return 1;
    }

    n++;
    if (cfg.reopen_every > 0 && (n % static_cast<std::size_t>(cfg.reopen_every) == 0)) {
      db.reset();
      policy.reset();
      db = OpenDB(cfg.db_path, false, cfg.bloom_bits, cfg.compression, &policy);
    }
    if (crash_mode && static_cast<int>(n) >= cfg.crash_after) {
      std::cerr << "leveldb_kvtool: intentional crash at op=" << n << "\n";
      std::fflush(stdout);
      std::fflush(stderr);
      std::_Exit(97);
    }
  }

  if (crash_mode) {
    std::cerr << "workload finished before --crash-after=" << cfg.crash_after << " (applied=" << n << ")\n";
    return 1;
  }

  db.reset();
  policy.reset();
  return 0;
}

int RunTranscript(const Config& cfg) {
  Debug("open db");
  std::unique_ptr<const leveldb::FilterPolicy> policy;
  std::unique_ptr<leveldb::DB> db = OpenDB(cfg.db_path, false, cfg.bloom_bits, cfg.compression, &policy);

  std::ofstream out(cfg.out_path, std::ios::binary | std::ios::trunc);
  if (!out.is_open()) {
    std::cerr << "failed to open output: " << cfg.out_path << "\n";
    return 1;
  }

  leveldb::ReadOptions ro;

  {
    Debug("forward iterator start");
    std::unique_ptr<leveldb::Iterator> it(db->NewIterator(ro));
    std::uint64_t n = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      out.put('F');
      WriteBytes(&out, it->key());
      WriteBytes(&out, it->value());
      n++;
    }
    MustOK(it->status(), "forward iterator");
    Debug("forward iterator done n=" + std::to_string(n));
  }

  {
    Debug("reverse iterator start");
    std::unique_ptr<leveldb::Iterator> it(db->NewIterator(ro));
    std::uint64_t n = 0;
    for (it->SeekToLast(); it->Valid(); it->Prev()) {
      out.put('R');
      WriteBytes(&out, it->key());
      WriteBytes(&out, it->value());
      n++;
    }
    MustOK(it->status(), "reverse iterator");
    Debug("reverse iterator done n=" + std::to_string(n));
  }

  std::ifstream in(cfg.queries_path);
  if (!in.is_open()) {
    std::cerr << "failed to open queries: " << cfg.queries_path << "\n";
    return 1;
  }

  Debug("seek/get query iterator start");
  std::unique_ptr<leveldb::Iterator> seek_it(db->NewIterator(ro));

  std::string line;
  while (std::getline(in, line)) {
    char op;
    std::string tokA;
    std::string tokB;
    bool ok = false;
    try {
      ok = ReadOpLine(line, &op, &tokA, &tokB);
    } catch (const std::exception& ex) {
      std::cerr << ex.what() << "\n";
      return 1;
    }
    if (!ok) continue;

    std::string key;
    if (!DecodeHex(tokA, &key)) {
      std::cerr << "bad query key: " << tokA << "\n";
      return 1;
    }

    if (op == 'G') {
      std::string value;
      leveldb::Status s = db->Get(ro, key, &value);
      out.put('G');
      WriteBytes(&out, key);
      if (s.ok()) {
        out.put(1);
        WriteBytes(&out, value);
      } else if (s.IsNotFound()) {
        out.put(0);
      } else {
        throw std::runtime_error("get failed: " + s.ToString());
      }
      continue;
    }

    if (op == 'S') {
      seek_it->Seek(key);

      out.put('S');
      WriteBytes(&out, key);
      if (seek_it->Valid()) {
        out.put(1);
        WriteBytes(&out, seek_it->key());
        WriteBytes(&out, seek_it->value());
      } else {
        out.put(0);
      }

      out.put('P');
      if (!seek_it->Valid()) {
        out.put(2);  // skipped
      } else {
        seek_it->Prev();
        if (seek_it->Valid()) {
          out.put(1);
          WriteBytes(&out, seek_it->key());
          WriteBytes(&out, seek_it->value());
        } else {
          out.put(0);
        }
      }

      continue;
    }

    std::cerr << "invalid query op: " << op << "\n";
    return 1;
  }

  MustOK(seek_it->status(), "seek iterator");
  Debug("seek/get query iterator done");

  seek_it.reset();
  db.reset();
  policy.reset();
  out.close();
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  Config cfg;
  if (!ParseArgs(argc, argv, &cfg)) {
    Usage();
    return 1;
  }

  try {
    if (cfg.cmd == "apply") {
      return RunApply(cfg, false);
    }
    if (cfg.cmd == "apply-crash") {
      return RunApply(cfg, true);
    }
    if (cfg.cmd == "transcript") {
      return RunTranscript(cfg);
    }
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << "\n";
    return 1;
  }

  Usage();
  return 1;
}
