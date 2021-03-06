// inode layer interface.

#ifndef inode_h
#define inode_h

#include <stdint.h>
#include <vector>
#include <algorithm>
#include "extent_protocol.h" // TODO: delete it

#define DISK_SIZE  1024*1024*16
#define BLOCK_SIZE 512
#define BLOCK_NUM  (DISK_SIZE/BLOCK_SIZE)

typedef uint32_t blockid_t;

// disk layer -----------------------------------------

class disk {
 private:
  //unsigned char blocks[BLOCK_NUM][BLOCK_SIZE];

 public:
  disk();
  disk(disk *);
  void read_block(uint32_t id, char *buf);
  void write_block(uint32_t id, const char *buf);
  unsigned char blocks[BLOCK_NUM][BLOCK_SIZE];
};

// block layer -----------------------------------------

typedef struct superblock {
  uint32_t size;
  uint32_t nblocks;
  uint32_t ninodes;
} superblock_t;

class block_manager {
 private:
  disk *d;
  std::map <uint32_t, int> using_blocks;
  std::vector<disk *> pversion;
  int version;
  
 public:
  block_manager();
  struct superblock sb;

  uint32_t alloc_block();
  void free_block(uint32_t id);
  void read_block(uint32_t id, char *buf);
  void write_block(uint32_t id, const char *buf);
  
  void commit();
  void undo();
  void redo();
};

// inode layer -----------------------------------------

// begin from 1
#define INODE_NUM  1024

// Inodes per block.
#define IPB           1
//(BLOCK_SIZE / sizeof(struct inode))
// IPB=1 is important for thread-safe

// reserved blocks
#define RESERVED_BLOCK(ninodes, nblocks)     (2 + ((nblocks) + BPB - 1)/BPB + ((ninodes) + IPB - 1)/IPB)

// Block containing inode i
#define IBLOCK(i, nblocks)     (2 + ((nblocks) + BPB - 1)/BPB + ((i)-1)/IPB)

// Bitmap bits per block
#define BPB           (BLOCK_SIZE*8)

// Block containing bit for block b
#define BBLOCK(b) ((b)/BPB + 2)

#define NDIRECT 32
#define NINDIRECT (BLOCK_SIZE / sizeof(uint))
#define MAXFILE (NDIRECT + NINDIRECT)

typedef struct inode {
  short type; // 0 for free
  unsigned int size;
  unsigned int atime;
  unsigned int mtime;
  unsigned int ctime;
  blockid_t blocks[NDIRECT+1];   // Data block addresses
} inode_t;

class inode_manager {
 private:
  block_manager *bm;
  struct inode* get_inode(uint32_t inum);
  void put_inode(uint32_t inum, struct inode *ino);

 public:
  inode_manager();
  uint32_t alloc_inode(uint32_t type);
  void free_inode(uint32_t inum);
  void read_file(uint32_t inum, char **buf, int *size);
  void write_file(uint32_t inum, const char *buf, int size);
  void remove_file(uint32_t inum);
  void getattr(uint32_t inum, extent_protocol::attr &a);
  
  void commit();
  void undo();
  void redo();
};

#endif

