#include <stdint.h>

#include <string>
#include <unordered_map>
#include <vector>

#include "fs.h"
#include "kv.h"

void KVStore::savekv(MemoryEntry * ment){
  for(const auto & each : mp){
    int log_size[2] = {int(each.first.size()), int(each.second.size())};
    write(ment, (char *)log_size, 8);
    write(ment, each.first.c_str(), log_size[0]);
    write(ment, each.second.c_str(), log_size[1]);
  }
}

KVStore::KVStore(const std::string &dir, bool format) : dir(dir) {
  init(dir, format);
  file = open("current");
  if (file == nullptr) {
    file = open("new");
    if (file == nullptr) {
      file = create("current");
    } else {
      rename_file("new", "current");
    }
  } else {
    remove_file("new");
  }
  offset = 0;
  bool broken = false;
  while (1) {
    int len[2];
    std::string key, val;
    if (eof(file)) {
      break;
    }
    int read_size = read(file, (char *)len, 8);
    if(read_size != 8){
      broken = true;
      break;
    }
    key.resize(len[0]);
    read_size = read(file, &key[0], len[0]);
    if(read_size != len[0]){
      broken = true;
      break;
    }
    if (len[1]) {
      val.resize(len[1]);
      read_size = read(file, &val[0], len[1]);
      if(read_size != len[1]){
        broken = true;
        break;
      }
      mp[key] = val;
    } else {
      mp.erase(key);
    }
    offset += 8 + len[0] + len[1];
  }
  if(broken){
    close(file);
    MemoryEntry * newfile = create("new");
    savekv(newfile);
    remove_file("current");
    rename_file("new", "current");
    file = newfile;
    newfile = nullptr;
  }
}

KVStore::~KVStore() {
  close(file);
  MemoryEntry * newfile = create("new");
  savekv(newfile);
  remove_file("current");
  rename_file("new", "current");
  close(newfile);
  destroy();
}

int KVStore::size() const { return mp.size(); }

std::string KVStore::get(const std::string &key) const {
  auto iter = mp.find(key);
  if (iter != mp.end()) {
    return iter->second;
  }
  return "";
}

bool KVStore::put(const std::string &key, const std::string &val) {
  int log_size[2] = {int(key.size()), int(val.size())};
  offset += 8 + log_size[0];
  write(file, (char *)log_size, 8);
  write(file, key.c_str(), log_size[0]);
  write(file, val.c_str(), log_size[1]);
  mp[key] = val;
  return true;
}

bool KVStore::remove(const std::string &key) {
  auto iter = mp.find(key);
  if (iter != mp.end()) {
    int log_size[2] = {int(key.size()), 0};
    write(file, (char *)log_size, 8);
    write(file, key.c_str(), log_size[0]);
    offset += 8 + log_size[0];
    mp.erase(iter);
    return true;
  }
  return false;
}

std::vector<std::string> KVStore::list() const {
  std::vector<std::string> ret;
  for (const auto &each : mp) {
    ret.push_back(each.first);
  }
  return ret;
}