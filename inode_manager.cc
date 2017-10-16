#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
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
    //std::cout<<"void disk::read_block begin"<<std::endl;
    if(id<0||id>BLOCK_NUM)
		return;
    if(buf==NULL)
		return;
    memcpy(buf,blocks[id],BLOCK_SIZE);
    //std::cout<<"void disk::read_block end"<<std::endl;
}

void
disk::write_block(blockid_t id, const char *buf)
{
  /*
   *your lab1 code goes here.
   *hint: just like read_block
   */
    if(id<0||id>BLOCK_NUM)
		return;
    memcpy(blocks[id],buf,BLOCK_SIZE);
    //std::cout<<"[inode_manager] void disk::write_block blocks[id]="<<blocks[id]<<std::endl;
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

   *hint: use macro IBLOCK and BBLOCK.
          use bit operation.
          remind yourself of the layout of disk.
   */
	//std::cout<<"[inode_manager] blockid_t block_manager::alloc_block() begin"<<std::endl;
    int start=1+BFB+INODE_NUM/IPB;
    //std::cout<<"[inode_manager] start:"<<start<<std::endl;
    for(int i=start;i<start+INODE_NUM;++i)
    {
		if(using_blocks.find(i)==using_blocks.end())
		{
			using_blocks[i]=1;
			return i;
		}
    }
    return 0;
    /*char buf[BLOCK_SIZE];
    for(uint32_t i=2;i<2+sb.nblocks/BPB;i++)
    {
	d->read_block(i,buf);
        for(int j=0;j<BLOCK_SIZE;++j)
        {
	    for(int k=0;k<8;++k)
	    {
		int bit=0x1<<k;
		if((buf[j]&bit)==0)
		    return (i-2)*BPB+j*8+k+1+sb.nblocks/BPB+INODE_NUM/IPB+1;
	    }
	}
    }
    return 0;*/
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your lab1 code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
    using_blocks.erase(id);
    /*char buf[BLOCK_SIZE];
    d->read_block(BBLOCK(id),buf);
    int left=id-id/BPB*BPB;
    int bit=left-left/8*8;
    buf[left/8]=buf[left/8]^(0x1<<bit);*/

}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

    /*d->write_block(1,(char *)&sb);
    int BOB=sb.nblocks/BPB;
    int fullBlocks=1+BLOCK_NUM/BPB+INODE_NUM/IPB;
    char buf[BOB][BLOCK_SIZE]={0x0};
    for(int i=0;i<fullBlocks;++i)
    {
	int bit=8-(i-i/8*8+1);
        buf[i/8/BLOCK_SIZE][i/8]=buf[i/8/BLOCK_SIZE][i/8]|(0x1<<bit);
    }
    for(int i=0;i<BOB;++i)
	d->write_block(i+2,buf[i]);*/
}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
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
    
   * if you get some heap memory, do not forget to free it.
   */
    //printf("uint32_t inode_manager::alloc_inode\n");
    timespec time;
    for(int i=1;i<=INODE_NUM;i++)
    {
		inode *tmp=get_inode(i);
		if(tmp==NULL)
		{
			clock_gettime(CLOCK_REALTIME,&time);
			inode *newInode=new inode();
			newInode->type=type;
	    	put_inode(i,newInode);
            delete newInode;
	    	return i;
		}
    }
    return 0;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your lab1 code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   * do not forget to free memory if necessary.
   */
	//std::cout<<"[inode_manager] void inode_manager::free_inode begin"<<std::endl;
	inode *tmp=get_inode(inum);
	tmp->type=0;tmp->size=0;
	put_inode(inum,tmp);
	//std::cout<<"[inode_manager] void inode_manager::free_inode end"<<std::endl;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
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

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your lab1 code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
	//std::cout<<"[inode_manager] void inode_manager::read_file begin"<<std::endl;
    inode *tmp=get_inode(inum);
    *size=tmp->size;
    int blocks=ceil((double)tmp->size/BLOCK_SIZE);
    //std::cout<<"[inode_manager] blocks:"<<blocks<<std::endl;
    /*buf_out=new char *[blocks];
    for(int i=0;i<blocks;++i)
		buf_out[i]=new char[BLOCK_SIZE];
	//std::cout<<"[inode_manager] for(int i=0;i<blocks;++i)"<<std::endl;
    int i;
    for(i=0;i<MIN(blocks,NDIRECT);++i)
		bm->read_block(tmp->blocks[i],buf_out[i]);
	//std::cout<<"[inode_manager] for(i=0;i<MIN(blocks,NDIRECT);++i)"<<std::endl;*/
	*buf_out=new char[blocks*BLOCK_SIZE];
	char *read_buf=*buf_out;
	for(int i=0;i<MIN(blocks,NDIRECT);++i)
	{
		bm->read_block(tmp->blocks[i],read_buf);
		read_buf+=BLOCK_SIZE;
	}
    if(blocks>NDIRECT)
    {
		int indirect=tmp->blocks[NDIRECT];
		char buf[BLOCK_SIZE];
		bm->read_block(indirect,buf);
		int left=blocks-NDIRECT;
		int indirBlock;
		for(int j=0;j<left;++j)
		{
		    indirBlock=*(int *)&buf[j*4];
		    //std::cout<<"[inode_manager] indirBlock:"<<indirBlock<<std::endl;
		    bm->read_block(indirBlock,read_buf);
		    read_buf+=BLOCK_SIZE;
		}
    }
    //std::cout<<"[inode_manager] void inode_manager::read_file end"<<std::endl;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your lab1 code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode.
   * you should free some blocks if necessary.
   */
	//std::cout<<"[inode_manager] void inode_manager::write_file begin"<<std::endl;
	//std::cout<<"[inode_manager] void inode_manager::write_file size="<<size<<std::endl;
	timespec time;
	clock_gettime(CLOCK_REALTIME,&time);
    inode *tmp=get_inode(inum);
    int origBlocks=ceil((double)tmp->size/BLOCK_SIZE);
    int needBlocks=ceil((double)size/BLOCK_SIZE);
    //std::cout<<"[inode_manager] origBlocks:"<<origBlocks<<std::endl;
    //std::cout<<"[inode_manager] needBlocks:"<<needBlocks<<std::endl;
    const char *tmpbuf=buf;
    if(needBlocks<=origBlocks)
    {
    	//std::cout<<"[inode_manager] needBlocks<=origBlocks"<<std::endl;
		for(int i=0;i<MIN(needBlocks,NDIRECT);++i)
		{
	    	bm->write_block(tmp->blocks[i],tmpbuf);
	    	tmpbuf+=BLOCK_SIZE;
		}
		if(needBlocks>NDIRECT)
		{
	    	int indirect=tmp->blocks[NDIRECT];
	    	char read_buf[BLOCK_SIZE];
	    	bm->read_block(indirect,read_buf);
			int left=needBlocks-NDIRECT;
			int indirBlock;
			for(int i=0;i<left;++i)
			{
				indirBlock=*(int *)&read_buf[i*4];
	   			bm->write_block(indirBlock,tmpbuf);
				tmpbuf+=BLOCK_SIZE;
			}
		}
	    else
		{
			if(origBlocks>NDIRECT)
			{
				int indirect=tmp->blocks[NDIRECT];
				char read_buf[BLOCK_SIZE];
	    		bm->read_block(indirect,read_buf);
				int left=origBlocks-NDIRECT;
				int indirBlock;
				for(int i=0;i<left;++i)
				{
					indirBlock=*(int *)&read_buf[i*4];
	   				bm->free_block(indirBlock);
				}
				bm->free_block(indirect);
			}
		}
    }
    else//needBlocks>origBlocks
    {
    	//std::cout<<"[inode_manager] needBlocks>origBlocks"<<std::endl;
    	if(origBlocks>NDIRECT)
    	{
    		for(int i=0;i<NDIRECT;++i)
			{
	    		bm->write_block(tmp->blocks[i],tmpbuf);
	    		tmpbuf+=BLOCK_SIZE;
			}
			int indirect=tmp->blocks[NDIRECT];
			char read_buf[BLOCK_SIZE];
	    	bm->read_block(indirect,read_buf);
			int left=origBlocks-NDIRECT;
			int indirBlock;
			for(int i=0;i<left;++i)
			{
				indirBlock=*(int *)&read_buf[i*4];
	   			bm->write_block(indirBlock,tmpbuf);
	   			tmpbuf+=BLOCK_SIZE;
			}
			int extra=needBlocks-origBlocks;
			uint32_t newBlock;
			for(int i=0;i<extra;++i)
			{
				newBlock=bm->alloc_block();
				*(int *)&read_buf[i*4+left*4]=newBlock;
	   			bm->write_block(newBlock,tmpbuf);
	   			tmpbuf+=BLOCK_SIZE;
			}
			bm->write_block(indirect,read_buf);
		}
		else//needBlocks>origBlocks;origBlocks<=NDIRECT
		{
			//std::cout<<"[inode_manager] origBlocks<=NDIRECT"<<std::endl;
			for(int i=0;i<origBlocks;++i)
			{
				//std::cout<<"[inode_manager] i="<<i<<std::endl;
	    		bm->write_block(tmp->blocks[i],tmpbuf);
	    		tmpbuf+=BLOCK_SIZE;
			}
			uint32_t newBlock;
			if(needBlocks>NDIRECT)
			{
				//std::cout<<"[inode_manager] needBlocks>NDIRECT"<<std::endl;
				int left=NDIRECT-origBlocks;
				for(int i=0;i<left;++i)
				{
					newBlock=bm->alloc_block();
	    			bm->write_block(newBlock,tmpbuf);
	    			tmp->blocks[origBlocks+i]=newBlock;
	    			tmpbuf+=BLOCK_SIZE;
				}
				int extraBlock=bm->alloc_block();
				char write_buf[BLOCK_SIZE]={0x0};
				left=needBlocks-NDIRECT;
				for(int i=0;i<left;++i)
				{
					newBlock=bm->alloc_block();
					*(int *)&write_buf[i*4]=newBlock;
					bm->write_block(newBlock,tmpbuf);
					tmpbuf+=BLOCK_SIZE;
				}
				bm->write_block(extraBlock,write_buf);
				tmp->blocks[NDIRECT]=extraBlock;
			}
			else//needBlocks>origBlocks;origBlocks<=NDIRECT;needBlocks<=NDIRECT
			{
				//std::cout<<"[inode_manager] needBlocks<=NDIRECT"<<std::endl;
				int left=needBlocks-origBlocks;
				//std::cout<<"[inode_manager] left="<<left<<std::endl;
				for(int i=0;i<left;++i)
				{
					newBlock=bm->alloc_block();
					//std::cout<<"[inode_manager] newBlock:"<<newBlock<<std::endl;
					tmp->blocks[origBlocks+i]=newBlock;
					bm->write_block(newBlock,tmpbuf);
					tmpbuf+=BLOCK_SIZE;
				}
			}
		}
	}
	tmp->size=size;
	tmp->mtime=time.tv_nsec;
	tmp->ctime=time.tv_nsec;
	put_inode(inum,tmp);
	//std::cout<<"[inode_manager] void inode_manager::write_file end"<<std::endl;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your lab1 code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
    //std::cout<<"[inode_manager] void inode_manager::getattr begin"<<std::endl;
    struct inode *tmp=get_inode(inum);
    if(tmp==NULL)
    	return;
    a.type=tmp->type;
    a.size=tmp->size;
    a.atime=tmp->atime;
    a.mtime=tmp->mtime;
    a.ctime=tmp->ctime;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your lab1 code goes here
   * note: you need to consider about both the data block and inode of the file
   * do not forget to free memory if necessary.
   */
	//std::cout<<"[inode_manager] void inode_manager::remove_file begin"<<std::endl;
	inode *tmp=get_inode(inum);
	int blocks=ceil((double)tmp->size/BLOCK_SIZE);
	//std::cout<<"[inode_manager] blocks:"<<blocks<<std::endl;
	for(int i=0;i<MIN(blocks,NDIRECT);++i)
		bm->free_block(tmp->blocks[i]);
	if(blocks>NDIRECT)
	{
		int indir=tmp->blocks[NDIRECT];
		char buf[BLOCK_SIZE];
		bm->read_block(indir,buf);
		int left=blocks-NDIRECT;
		int indirBlock;
		for(int i=0;i<left;++i)
		{
			indirBlock=*(int *)&buf[i*4];
			bm->free_block(indirBlock);
		}
		bm->free_block(indir);
	}
	free_inode(inum);
	//std::cout<<"[inode_manager] void inode_manager::remove_file end"<<std::endl;
}
