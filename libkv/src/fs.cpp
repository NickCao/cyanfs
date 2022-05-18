#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <string>
#include <utility>
#include <vector>

#define min(a, b) ((a) < (b) ? (a) : (b))

#include "fs.h"
#include "lru.h"

static struct superblock sb __attribute__((aligned(4096)));
static Data databuf[1024] __attribute__((aligned(4096)));

static int fd = 0;

void create_disk(const std::string &path){
    fd = open(path.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0777);
    assert(fd >= 0);
    sb.magic_number = MAGICNUM;
    sb.block_size = DISK_SIZE / BSIZE;
    memset(sb.entries, 0, sizeof(sb.entries));
    write(fd, &sb, sizeof(sb));
    char *tmp = new char[DISK_SIZE - sizeof(sb)];
    memset(tmp, 0, DISK_SIZE - sizeof(sb));
    write(fd, tmp, DISK_SIZE - sizeof(sb));
    delete[] tmp;
    close(fd);
}

void init(const std::string &path, bool format) {
    fd = open(path.c_str(), O_RDWR | O_NOATIME | O_DIRECT, 0777);
    if(fd == -1){
        create_disk(path);
        fd = open(path.c_str(), O_RDWR | O_NOATIME | O_DIRECT, 0777);
    }
    assert(fd >= 0);
    read(fd, &sb, sizeof(superblock));
    if(sb.magic_number != MAGICNUM || format){
        sb.magic_number = MAGICNUM;
        sb.block_size = DISK_SIZE / BSIZE;
        memset(sb.entries, 0, sizeof(sb.entries));
        lseek(fd, 0, SEEK_SET);
        write(fd, &sb, sizeof(sb));
    }
}


void write_entry() {
    lseek(fd, 0, SEEK_SET);
    int size = write(fd, &sb, BSIZE);
    assert(size == BSIZE);
}

int find_entry(){
    for(int i = 0; i < 2; i++){
        if(sb.entries[i].used == 0){
            return i;
        }
    }
    return -1;
}

MemoryEntry *look_up(const char *name){
    for (int i = 0; i < 2; i++) {
        entry *cur = &sb.entries[i];
        if (cur->used && strcmp(cur->name, name) == 0){
            MemoryEntry *ret = new MemoryEntry();
            ret->pos = i;
            ret->offset = 0;
            return ret;
        }
    }
    return NULL;
}

MemoryEntry *create(const char *name) {
    MemoryEntry *res = look_up(name);
    if (res != NULL)
    {
        return res;
    }
    auto pos = find_entry();
    if (pos == -1)
    {
        return NULL;
    }
    entry *cur = &(sb.entries[pos]);
    strcpy(cur->name, name);
    if(pos == 0){
        cur->block_start = sizeof(sb) / BSIZE;
    } else {
        cur->block_start = sb.block_size / 2;
    }
    cur->used = 1;
    cur->fsize = 0;
    write_entry();
    res = new MemoryEntry();
    res->pos = pos;
    res->offset = 0;
    return res;
}

void read_disk(int block_no, int block_size){
    int size = block_size * BSIZE;
    lseek(fd, block_no * BSIZE, SEEK_SET);
    int read_size = read(fd, databuf, size);
    assert(size == read_size);
}

void write_disk(int block_no, int block_size) {
    int size = block_size * BSIZE;
    lseek(fd, block_no * BSIZE, SEEK_SET);
    int write_size = write(fd, &databuf, size);
    assert(write_size == size);
}

int write(MemoryEntry *ment, const char *buffer, int len){
    entry *ent = &(sb.entries[ment->pos]);
    int p = 0;
    while (p < len) {
        int current_from = ent->fsize % BSIZE;
        int last_block = ent->block_start + (ent->fsize / BSIZE);
        if (current_from != 0){
            read_disk(last_block, 1);
        }
        int write_size = min(sizeof(databuf) - current_from, len - p);
        memcpy(databuf[0].buf + current_from, buffer + p, write_size);
        p += write_size;
        ent->fsize += write_size;
        int block_size = (current_from + write_size + BSIZE - 1) / BSIZE;
        write_disk(last_block, block_size);
    }
    write_entry();
    return len;
}

bool eof(MemoryEntry *ment){
    if (ment->offset == sb.entries[ment->pos].fsize)
    {
        return true;
    }
    return false;
}

int read(MemoryEntry *ment, char *buffer, int len){
    entry *ent = &(sb.entries[ment->pos]);
    int fsize = ent->fsize;
    int p = 0;
    int current = ment->offset;
    while (p < len && current < fsize){
        int current_from = current % BSIZE;
        int cur_block = (ment->offset / BSIZE) + sb.entries[ment->pos].block_start;
        int read_size = min(min(sizeof(databuf) - current_from, len - p), fsize - current);
        int read_block = (current_from + read_size + BSIZE - 1) / BSIZE;
        read_disk(cur_block, read_block);
        memcpy(buffer + p, databuf[0].buf + current_from, read_size);
        p += read_size;
        current += read_size;
        ment->offset += read_size;
    }
    return p;
}

int seek(MemoryEntry *ment, u64 offset, int from){
    entry *ent = &(sb.entries[ment->pos]);
    if (from == SEEK_C) {
        offset += ment->offset;
    }
    if (offset > ent->fsize) {
        offset = ent->fsize;
    }
    if(offset < 0){
        offset = 0;
    }
    ment->offset = offset;
    return ment->offset;
}

MemoryEntry *open(const char *name) {
    MemoryEntry *ret = look_up(name);
    return ret;
}

int close(MemoryEntry *p) {
    if (p != NULL) {
        delete p;
        return 0;
    }
    return -1;
}

void destroy() {
    close(fd);
}

u64 fsize(MemoryEntry *ent){
    return sb.entries[ent->pos].fsize;
}

bool remove_file(const char *filename) {
    MemoryEntry *mement = look_up(filename);
    if (mement == nullptr) {
        return false;
    }
    memset(&sb.entries[mement->pos], 0, sizeof(entry));
    write_entry();
    delete mement;
    return true;
}

bool rename_file(const char *oldname, const char *newname) {
    MemoryEntry *mement = look_up(oldname);
    MemoryEntry *mementnew = look_up(newname);
    if (mement == nullptr) {
        return false;
    }
    if (mementnew) {
        delete mement;
        delete mementnew;
        return false;
    }
    strcpy(sb.entries[mement->pos].name, newname);
    write_entry();
    delete mement;
    return true;
}
