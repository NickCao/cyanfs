#ifndef __FS_H__
#define __FS_H__

#include <string>
#include <vector>

typedef unsigned long long int u64;
typedef long long int i64;
typedef unsigned char u8;

const u64 DISK_SIZE = 1 << 30;
const u64 BSIZE = 512;
const u64 MAGICNUM = 0x202205012;

struct entry
{
    char name[128 - 2 * sizeof(u64) - sizeof(int)];
    int used;
    i64 block_start;
    u64 fsize;
};

static_assert((sizeof(entry) % 128 == 0), "entry_size");
struct superblock
{
    u64 magic_number;
    u64 block_size;
    entry entries[2];
    char pad[BSIZE - ((sizeof(magic_number) + sizeof(entries) + sizeof(block_size)) % BSIZE)];
};

// When adding a new filed into MemoryEntry, remember to initial it in
// open/create
struct MemoryEntry{
    int pos;     // entry num
    int offset;  // read pointer
};

struct Data{
    char buf[BSIZE];
};

enum
{
    SEEK_S,
    SEEK_C
};

void create_disk(const std::string &path);
void init(const std::string &path, bool format);
MemoryEntry *create(const char *);
int write(MemoryEntry *, const char *, int);
int read(MemoryEntry *, char *, int);
MemoryEntry *open(const char *);
int close(MemoryEntry *);
void destroy();
int seek(MemoryEntry *, u64, int);
bool remove_file(const char *);
bool rename_file(const char *oldname, const char *newname);
bool eof(MemoryEntry *);
u64 fsize(MemoryEntry *ent);

#endif