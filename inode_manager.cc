#include <pthread.h>
#include <cstring>
#include <ctime>
#include <time.h>
#include "inode_manager.h"

// disk layer -----------------------------------------

static time_t gettimesec()
{
	struct timespec tp;
	clock_gettime(CLOCK_REALTIME,&tp);
	return tp.tv_sec;
}

disk::disk()
{
  pthread_t id;
  int ret;
  bzero(blocks, sizeof(blocks));

  ret = pthread_create(&id, NULL, test_daemon, (void*)blocks);
  if(ret != 0)
	  printf("FILE %s line %d:Create pthread error\n", __FILE__, __LINE__);
}

disk::disk(disk *d)
{
	for(int i=0;i<BLOCK_NUM;++i)
		memcpy(blocks[i],d->blocks[i],BLOCK_SIZE);
}

void
disk::read_block(blockid_t id, char *buf)
{
  /*
   *your lab1 code goes here.
   *if id is smaller than 0 or larger than BLOCK_NUM 
   *or buf is null, just return.
   *put the content of target block into buf.
   *hint: use memcpy
  */
	//std::cout<<gettimesec()<<"[inode_manager]disk::read_block id="<<id<<std::endl;
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(buf, blocks[id], BLOCK_SIZE);
	//std::cout<<gettimesec()<<"[inode_manager] buf=";string2hex(buf,BLOCK_SIZE);printf("\n");
}

void
disk::write_block(blockid_t id, const char *buf)
{
  /*
   *your lab1 code goes here.
   *hint: just like read_block
  */
	//std::cout<<gettimesec()<<"[inode_manager]disk::write_block id="<<id<<std::endl;
	//std::cout<<gettimesec()<<"[inode_manager] buf=";string2hex(buf,BLOCK_SIZE);printf("\n");
   if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your lab1 code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
	int start=2+BFB+INODE_NUM/IPB;
	for(int i=start;i<BLOCK_NUM;i+=2)
	{
		if(using_blocks.find(i)==using_blocks.end())
		{
			using_blocks[i]=1;
			using_blocks[++i]=1;
			return --i;
		}
	}
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your lab1 code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  	using_blocks.erase(id);
  	using_blocks.erase(++id);
}

block_manager::block_manager()
{
  d = new disk();

  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;
  
  version=-1;
}

void
block_manager::read_block(uint32_t id, char *buf)
{
	//std::cout<<gettimesec()<<"[inode_manager]block_manager::read_block id="<<id<<std::endl;
	char buffer[2*BLOCK_SIZE];
	d->read_block(id,buffer);
	char *cur=buffer+BLOCK_SIZE;
	d->read_block(++id,cur);
	deECC(buffer,buf);
	//std::cout<<gettimesec()<<"[inode_manager]block_manager::read_block buf="<<buf<<std::endl;
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
	//std::cout<<gettimesec()<<"[inode_manager]block_manager::write_block id="<<id<<std::endl;
	//std::cout<<gettimesec()<<"[inode_manager]block_manager::write_block buf="<<buf<<std::endl;
	char *buffer=enECC(buf);
	//std::cout<<gettimesec()<<"[inode_manager] buffer=";string2hex(buffer,2*BLOCK_SIZE);printf("\n");
	d->write_block(id, buffer);
	if(using_blocks.find(++id)==using_blocks.end())
		using_blocks[id]=1;
	char *tmp=buffer+BLOCK_SIZE;
	d->write_block(id,tmp);
	delete buffer;
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your lab1 code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
	//std::cout<<gettimesec()<<"[inode_manager]inode_manager::alloc_inode type="<<type<<std::endl;
  char buf[BLOCK_SIZE];
  uint32_t cur = 1;
  while (cur <= bm->sb.ninodes) {
    bm->read_block(IBLOCK(cur, bm->sb.nblocks), buf);
    for (int i = 0; i < IPB && cur <= bm->sb.ninodes; ++i) {
      inode_t * ino = (inode_t *)buf + i;
      if (ino->type == 0) {
        ino->type = type;
        ino->size = 0;
        ino->atime = std::time(0);
        ino->mtime = std::time(0);
        ino->ctime = std::time(0);
        bm->write_block(IBLOCK(cur, bm->sb.nblocks), buf);
        return cur;
      }
      cur+=2;
    }
  }
  printf("\tim: error! out of inodes\n");
  exit(0);
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your lab1 code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
	//std::cout<<gettimesec()<<"[inode_manager]inode_manager::free_inode inum="<<inum<<std::endl;
  char buf[BLOCK_SIZE];
  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  inode_t * ino = (inode_t *)buf + (inum - 1) % IPB;
  if (ino->type == 0) {
    printf("\tim: error! inode is already freed\n");
    exit(0);
  } else {
    ino->type = 0;
    bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
  }
}

/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum <= 0 || inum > INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
	//std::cout<<gettimesec()<<"[inode_manager]inode_manager::put_inode inum="<<inum<<std::endl;
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your lab1 code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_Out
   */
	//std::cout<<gettimesec()<<"[inode_manager]inode_manager::read_file inum="<<inum<<std::endl;
  char block[BLOCK_SIZE];
  inode_t * ino = get_inode(inum);
  char * buf = (char *)malloc(ino->size);
  unsigned int cur = 0;
  for (int i = 0; i < NDIRECT && cur < ino->size; ++i) {
    if (ino->size - cur > BLOCK_SIZE) {
      bm->read_block(ino->blocks[i], buf + cur);
      cur += BLOCK_SIZE;
    } else {
      int len = ino->size - cur;
      bm->read_block(ino->blocks[i], block);
      memcpy(buf + cur, block, len);
      cur += len;
    }
  }

  if (cur < ino->size) {
    char indirect[BLOCK_SIZE];
    bm->read_block(ino->blocks[NDIRECT], indirect);
    for (unsigned int i = 0; i < NINDIRECT && cur < ino->size; ++i) {
      blockid_t ix = *((blockid_t *)indirect + i);
      if (ino->size - cur > BLOCK_SIZE) {
        bm->read_block(ix, buf + cur);
        cur += BLOCK_SIZE;
      } else {
        int len = ino->size - cur;
        bm->read_block(ix, block);
        memcpy(buf + cur, block, len);
        cur += len;
      }
    }
  }

  *buf_out = buf;
  *size = ino->size;
  ino->atime = std::time(0);
  ino->ctime = std::time(0);
  put_inode(inum, ino);
  free(ino);
}

void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your lab1 code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
	//std::cout<<gettimesec()<<"[inode_manager]inode_manager::write_file inum="<<inum<<std::endl;
  char block[BLOCK_SIZE];
  char indirect[BLOCK_SIZE];
  inode_t * ino = get_inode(inum);
  unsigned int old_block_num = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  unsigned int new_block_num = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;

  if (old_block_num > new_block_num) {
    if (new_block_num > NDIRECT) {
      bm->read_block(ino->blocks[NDIRECT], indirect);
      for (unsigned int i = new_block_num; i < old_block_num; ++i) {
        bm->free_block(*((blockid_t *)indirect + (i - NDIRECT)));
      }
    } else {
      if (old_block_num > NDIRECT) {
        bm->read_block(ino->blocks[NDIRECT], indirect);
        for (unsigned int i = NDIRECT; i < old_block_num; ++i) {
          bm->free_block(*((blockid_t *)indirect + (i - NDIRECT)));
        }
        bm->free_block(ino->blocks[NDIRECT]);
        for (unsigned int i = new_block_num; i < NDIRECT; ++i) {
          bm->free_block(ino->blocks[i]);
        }
      } else {
        for (unsigned int i = new_block_num; i < old_block_num; ++i) {
          bm->free_block(ino->blocks[i]);
        }
      }
    }
  }

  if (new_block_num > old_block_num) {
    if (new_block_num <= NDIRECT) {
      for (unsigned int i = old_block_num; i < new_block_num; ++i) {
        ino->blocks[i] = bm->alloc_block();
      }
    } else {
      if (old_block_num <= NDIRECT) {
        for (unsigned int i = old_block_num; i < NDIRECT; ++i) {
          ino->blocks[i] = bm->alloc_block();
        }
        ino->blocks[NDIRECT] = bm->alloc_block();

        bzero(indirect, BLOCK_SIZE);
        for (unsigned int i = NDIRECT; i < new_block_num; ++i) {
          *((blockid_t *)indirect + (i - NDIRECT)) = bm->alloc_block();
        }
        bm->write_block(ino->blocks[NDIRECT], indirect);
      } else {
        bm->read_block(ino->blocks[NDIRECT], indirect);
        for (unsigned int i = old_block_num; i < new_block_num; ++i) {
          *((blockid_t *)indirect + (i - NDIRECT)) = bm->alloc_block();
        }
        bm->write_block(ino->blocks[NDIRECT], indirect);
      }
    }
  }

  int cur = 0;
  for (int i = 0; i < NDIRECT && cur < size; ++i) {
    if (size - cur > BLOCK_SIZE) {
      bm->write_block(ino->blocks[i], buf + cur);
      cur += BLOCK_SIZE;
    } else {
      int len = size - cur;
      memcpy(block, buf + cur, len);
      bm->write_block(ino->blocks[i], block);
      cur += len;
    }
  }

  if (cur < size) {
    bm->read_block(ino->blocks[NDIRECT], indirect);
    for (unsigned int i = 0; i < NINDIRECT && cur < size; ++i) {
      blockid_t ix = *((blockid_t *)indirect + i);
      if (size - cur > BLOCK_SIZE) {
        bm->write_block(ix, buf + cur);
        cur += BLOCK_SIZE;
      } else {
        int len = size - cur;
        memcpy(block, buf + cur, len);
        bm->write_block(ix, block);
        cur += len;
      }
    }
  }

  ino->size = size;
  ino->mtime = std::time(0);
  ino->ctime = std::time(0);
  put_inode(inum, ino);
  free(ino);
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your lab1 code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
	//std::cout<<gettimesec()<<"[inode_manager]inode_manager::getatt inum="<<inum<<std::endl;
  inode_t * ino = get_inode(inum);

  if (ino) {
    a.type = ino->type;
    a.atime = ino->atime;
    a.mtime = ino->mtime;
    a.ctime = ino->ctime;
    a.size = ino->size;

    free(ino);
  }
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your lab1 code goes here
   * note: you need to consider about both the data block and inode of the file
   */
	//std::cout<<gettimesec()<<"[inode_manager]inode_manager::remove_file inum="<<inum<<std::endl;
  inode_t * ino = get_inode(inum);
  unsigned int block_num = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  if (block_num <= NDIRECT) {
    for (unsigned int i = 0; i < block_num; ++i) {
      bm->free_block(ino->blocks[i]);
    }
  } else {
    for (int i = 0; i < NDIRECT; ++i) {
      bm->free_block(ino->blocks[i]);
    }
    char indirect[BLOCK_SIZE];
    bm->read_block(ino->blocks[NDIRECT], indirect);
    for (unsigned int i = 0; i < block_num - NDIRECT; ++i) {
      bm->free_block(*((blockid_t *)indirect + i));
    }
    bm->free_block(ino->blocks[NDIRECT]);
  }
  free_inode(inum);
  free(ino);
}

void inode_manager::commit()
{
	bm->commit();
}

void inode_manager::undo()
{
	bm->undo();
}

void inode_manager::redo()
{
	bm->redo();
}

void block_manager::commit()
{
	++version;
	disk *newd=new disk(d);
	pversion.push_back(d);
	d=newd;
}

void block_manager::undo()
{
	std::vector<disk *>::iterator it=find(pversion.begin(),pversion.end(),d);
	d=*(--it);
}

void block_manager::redo()
{
	std::vector<disk *>::iterator it=find(pversion.begin(),pversion.end(),d);
	d=*(++it);
}

char *block_manager::enECC(const char *src)
{
	//std::cout<<gettimesec()<<"[inode_manager]block_manager::enECC src=";string2hex(src);printf("\n");
	char *ret=new char[2*BLOCK_SIZE];
	char *cur=ret;
	char before,after;
	char p1,p2,p4;
	for(int i=0;i<BLOCK_SIZE;++i,++src,cur++)
	{
		before=*src;
		after=((before&0x80)>>2)|((before&0x40)>>3)|((before&0x20)>>3)|((before&0x10)>>3);
		p1=(((after&0x2)>>1)^((after&0x8)>>3)^((after&0x20)>>5))<<7;
		p2=(((after&0x2)>>1)^((after&0x4)>>2)^((after&0x20)>>5))<<6;
		p4=(((after&0x2)>>1)^((after&0x4)>>2)^((after&0x8)>>3))<<4;
		after|=p1|p2|p4;
		*cur=after;
		cur++;
		after=((before&0x8)<<2)|((before&0x4)<<1)|((before&0x2)<<1)|((before&0x1)<<1);
		p1=(((after&0x2)>>1)^((after&0x8)>>3)^((after&0x20)>>5))<<7;
		p2=(((after&0x2)>>1)^((after&0x4)>>2)^((after&0x20)>>5))<<6;
		p4=(((after&0x2)>>1)^((after&0x4)>>2)^((after&0x8)>>3))<<4;
		after|=p1|p2|p4;
		*cur=after;
	}
	//std::cout<<gettimesec()<<"[inode_manager]block_manager::enECC ret=";string2hex(ret);//printf("\n");
	return ret;
}

void block_manager::deECC(char *src,char *dst)
{
	//std::cout<<gettimesec()<<"[inode_manager]block_manager::deECC src=";string2hex(src);printf("\n");
	char *cur=dst;
	char before,ecc,after;
	for(int i=0;i<BLOCK_SIZE;++i,++src,cur++)
	{
		before=*src++;
		before=checkAndCorrect(before);
		after=((before&0x20)<<2)|((before&0x8)<<3)|((before&0x4)<<3)|((before&0x2)<<3);
		before=*src;
		before=checkAndCorrect(before);
		after|=((before&0x20)>>2)|((before&0x8)>>1)|((before&0x4)>>1)|((before&0x2)>>1);
		*cur=after;
	}
	//std::cout<<gettimesec()<<"[inode_manager]block_manager::deECC dst=";string2hex(dst);printf("\n");
}

char checkAndCorrect(char x)
{
	char c=x;
	bool p1=((c&0x80)==((((c&0x2)>>1)^((c&0x8)>>3)^((c&0x20)>>5))<<7));
	bool p2=((c&0x40)==((((c&0x2)>>1)^((c&0x4)>>2)^((c&0x20)>>5))<<6));
	bool p4=((c&0x10)==((((c&0x2)>>1)^((c&0x4)>>2)^((c&0x8)>>3))<<4));
	int p=p1<<2|p2<<1|p4;
	switch(p)
	{
		case 0:c^=0x2;break;
		case 1:c^=0x20;break;
		case 2:c^=0x8;break;
		case 3:c^=0x80;break;
		case 4:c^=0x4;break;
		case 5:c^=0x40;break;
		case 6:c^=0x10;break;
	}
	return c;
}

// for debug purpose
void string2hex(const char *src)
{
	int len=strlen(src);
	for(int i=0;i<len;++i,++src)
	{
		int tmp=int(*src);
		printf("0x%X ",tmp);
	}
}

void string2hex(const char *src,int size)
{
	for(int i=0;i<size;++i,++src)
	{
		int tmp=int(*src);
		printf("0x%X ",tmp);
	}
}

void char2hex(char c)
{
	unsigned int tmp=(unsigned int)c;
	printf("0x%X ",tmp);
}
