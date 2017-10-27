// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
  lc = new lock_client(lock_dst);
  if (ec->put(1, "") != extent_protocol::OK)
      printf("error init root dir\n"); // XYB: init root dir
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    //printf("isfile: %lld is a dir\n", inum);
    //std::cout<<"[yfs_client] bool yfs_client isfile "<<inum<<" is not a file"<<std::endl;
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool yfs_client::issymlink(inum inum)
{
	extent_protocol::attr a;
    if(ec->getattr(inum, a)!=extent_protocol::OK)
    {
        printf("error getting attr\n");
        return false;
    }
    if(a.type==extent_protocol::T_SYMLINK)
    {
        printf("issymlink: %lld is a symbolic link\n", inum);
        return true;
    } 
    printf("issymlink: %lld is not a symbolic link\n", inum);
    return false;
}

int yfs_client::symlink(inum parent,const char *name,const char *link,inum &ino_out)
{
	//std::cout<<"[yfs_client] int yfs_client::symlink begin"<<std::endl;
	if(!isdir(parent))
        return IOERR;
	bool found=false;
	if(lookup(parent,name,found,ino_out)!=OK)
		return IOERR;
	if(found)
		return EXIST;
	if(ec->create(extent_protocol::T_SYMLINK,ino_out)!=extent_protocol::OK)
		return IOERR;
    std::string buf;
	if(ec->get(parent,buf)!=extent_protocol::OK)
		return IOERR;
	std::string sname=name;
	std::string sinum=filename(ino_out);
	buf.append("/"+sname+"/"+sinum);
	//std::cout<<"[yfs_client] int yfs_client::symlink buf="<<buf<<std::endl;
	if(ec->put(parent,buf)!=extent_protocol::OK)
		return IOERR;
	/*size_t bytes_written;
	int result=write(ino_out,strlen(link),0,link,bytes_written);
	if(result==IOERR)
		return IOERR;*/
	if (ec->put(ino_out, std::string(link)) != extent_protocol::OK)
        return IOERR;
	//std::cout<<"[yfs_client] int yfs_client::symlink end"<<std::endl;
	return OK;
}

int yfs_client::getsymlink(inum inum, symlinkinfo &sin)
{
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK)
        return IOERR;
    sin.atime = a.atime;
    sin.mtime = a.mtime;
    sin.ctime = a.ctime;
    sin.size = a.size;
    return OK;
}

int yfs_client::readlink(inum inum,std::string &path)
{
	//std::cout<<"[yfs_client] int yfs_client::readlink begin"<<std::endl;
	//std::cout<<"[yfs_client] int yfs_client::readlink inum="<<inum<<std::endl;
	if (ec->get(inum, path) != extent_protocol::OK)
        return IOERR;
    //std::cout<<"[yfs_client] int yfs_client::readlink end"<<std::endl;
	return OK;
}

bool
yfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    //std::cout<<"[yfs_client] int yfs_client::isdir begin"<<std::endl;
    return ! isfile(inum)&&!issymlink(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */

    //std::cout<<"[yfs_client] int yfs_client::setattr begin"<<std::endl;
	std::string buf;
    fileinfo fin;
    if(getfile(ino, fin) != OK)
        return IOERR;
    if (read(ino,fin.size,0,buf) != OK)
        return IOERR;
    if(fin.size > size)
        buf = buf.substr(0,size);
    else if(fin.size < size)
        buf.append(size-fin.size,'\0');
    if(ec->put(ino,buf) != extent_protocol::OK)
        return IOERR;
    //std::cout<<"[yfs_client] int yfs_client::setattr end"<<std::endl;
    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    //std::cout<<"[yfs_client] int yfs_client::create begin"<<std::endl;
    //std::cout<<"[yfs_client] int yfs_client::create parent="<<parent<<std::endl;
	bool found = false;
	if(lookup(parent,name,found,ino_out) != OK)
		return IOERR;
	if (found)
		return EXIST;
	if( ec->create(extent_protocol::T_FILE, ino_out) != extent_protocol::OK)
		return IOERR;
    std::string buf;
	fileinfo fin;
	if(getfile(parent,fin)!= OK)
		return IOERR;
	if(read(parent,fin.size,0,buf) != OK)
		return IOERR;
	std::string sname = name;
	std::string sinum = filename(ino_out);
	buf.append("/" + sname + "/" + sinum);
	if(ec->put(parent,buf) != extent_protocol::OK)
		return IOERR;
	//std::cout<<"[yfs_client] int yfs_client::create end"<<std::endl;
    return r;
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    //std::cout<<"[yfs_client] int yfs_client::mkdir begin"<<std::endl;
	bool found=false;
	if(lookup(parent,name,found,ino_out)!=OK)
		return IOERR;
	if(found)
		return EXIST;
	if(ec->create(extent_protocol::T_DIR,ino_out)!=extent_protocol::OK)
		return IOERR;
	std::string buf;
	fileinfo fin;
	if(getfile(parent,fin)!=OK)
		return IOERR;
	if(read(parent,fin.size,0,buf)!=OK)
		return IOERR;
	std::string sname=name;
	std::string sinum=filename(ino_out);
	buf.append("/"+sname+"/"+sinum);
	if(ec->put(parent,buf) != extent_protocol::OK)
		return IOERR;
	//std::cout<<"[yfs_client] int yfs_client::mkdir end"<<std::endl;
    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */

	//std::cout<<"[yfs_client] int yfs_client::lookup begin"<<std::endl;
	//std::cout<<"[yfs_client] int yfs_client::lookup name="<<name<<std::endl;
	std::list<dirent> list;
	found=false;
	if(readdir(parent,list)!= OK)
		return IOERR;
	std::string sname = name;
	for (std::list<dirent>::iterator it = list.begin(); it !=list.end();it++)
	{
		if(sname.compare(it->name) == 0)
		{
			found = true;
			ino_out = it->inum;
			break;
		}
	}
	//std::cout<<"[yfs_client] int yfs_client::lookup end"<<std::endl;
    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */

	//std::cout<<"[yfs_client] int yfs_client::readdir begin"<<std::endl;
	std::string buf;
	fileinfo fin;
	if(!isdir(dir))
		return NOENT;
	if(getfile(dir,fin)!= OK)
		return IOERR;
	if(read(dir,fin.size,0,buf) != OK)
		return IOERR;
	char slist[buf.size()];
	strcpy(slist,buf.c_str());
	char *tokenPtr=strtok(slist,"/");
	dirent temp;
	while (tokenPtr!=NULL)
	{
		temp.name=tokenPtr;
		tokenPtr=strtok(NULL,"/");
		temp.inum=atoi(tokenPtr);
		//std::cout<<"[yfs_client] int yfs_client::readdir temp.name="<<temp.name<<std::endl;
		//std::cout<<"[yfs_client] int yfs_client::readdir temp.inum="<<temp.inum<<std::endl;
		list.push_back(temp);
		tokenPtr=strtok(NULL,"/");
	}
	//std::cout<<"[yfs_client] int yfs_client::readdir end"<<std::endl;
    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: read using ec->get().
     */

	if(ec->get(ino,data) != extent_protocol::OK)
		return IOERR;
	data = data.substr(off,size);
    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */

    /*std::cout<<"[yfs_client] int yfs_client::write begin"<<std::endl;
    std::cout<<"[yfs_client] int yfs_client::write size="<<size<<std::endl;
    std::cout<<"[yfs_client] int yfs_client::write off="<<off<<std::endl;
	std::string buf;
	fileinfo fin;
	if(getfile(ino,fin)!= OK)
		return IOERR;
	if(read(ino,fin.size,0,buf) != OK)
		return IOERR;
	std::cout<<"[yfs_client] int yfs_client::write buf.size()="<<buf.size()<<std::endl;
	std::string sdata = data;
	sdata = sdata.substr(0,size);
	int newSize = off+size;
	if(off > (int)buf.size())
	{	
		std::string write_buf;
		write_buf.resize(newSize);
		for(int i=0;i<(int)buf.size();i++)
			write_buf[i] = buf[i];
		for(int i=buf.size();i<off;i++)
			write_buf[i] = '\0';
		for(int i=off;i<newSize;i++)
			write_buf[i] = sdata[i-off];
		if(ec->put(ino,write_buf) != extent_protocol::OK)
			return IOERR;
		bytes_written=newSize-buf.size();
	}
	else
	{
		buf.resize(off+size);
		buf.replace(off,size,sdata);
		//std::cout<<"[yfs_client] int yfs_client::buf="<<buf<<std::endl;
		bytes_written = size;
		if(ec->put(ino,buf) != extent_protocol::OK)
			return IOERR;
	}*/
	//std::cout<<"[yfs_client] int yfs_client::write begin"<<std::endl;
	std::string buf;
    if (ec->get(ino, buf) != extent_protocol::OK)
        return IOERR;
    size_t len = buf.length();
    if ((size_t)(off+size) >= len)
        buf.resize(off+size);
    for (size_t i=off; i<off+size; i++)
        buf[i] = data[i-off];
    if (ec->put(ino, buf) != extent_protocol::OK)
        return IOERR;
	//std::cout<<"[yfs_client] int yfs_client::write end"<<std::endl;
    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */

    //std::cout<<"[yfs_client] int yfs_client::unlink begin"<<std::endl;
	bool found = false;
	inum ino_out;
	if(lookup(parent,name,found,ino_out) != OK)
		return IOERR;
	if (found == false)
		return NOENT;
	if(ec->remove(ino_out) != extent_protocol::OK)
		return IOERR;
	std::string buf;
	if(ec->get(parent,buf)!=extent_protocol::OK)
		return IOERR;
	size_t pos = buf.find(name);
	size_t size = strlen(name) + filename(ino_out).size() + 2;
	buf.erase(pos,size);
	if(ec->put(parent,buf) != extent_protocol::OK)
		return IOERR;
    return r;
}

