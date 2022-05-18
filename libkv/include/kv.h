#ifndef __KV_H__
#define __KV_H__

#include <stdint.h>
#include <string>
#include <unordered_map>
#include <vector>

struct MemoryEntry;

class KVStore {
private:
  int offset;
  MemoryEntry *file;
  std::string dir;
  std::unordered_map<std::string, std::string> mp;
  void savekv(MemoryEntry * ment);
public:
  KVStore(const std::string &dir, bool format);
  ~KVStore();
  int size() const;
  std::string get(const std::string &key) const;
  bool put(const std::string &key, const std::string &val);
  bool remove(const std::string &key);
  std::vector<std::string> list() const;
};

#endif
