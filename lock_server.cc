// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
	pthread_mutex_init(&mutex,NULL);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab4 code goes here
	std::cout<<"lock_server::acquire begin lid="<<lid<<std::endl;
	while(1)
	{
		if(lock_table[lid]==0)
		{
			pthread_mutex_lock(&mutex);
			if(lock_table[lid]==0)
			{
				lock_table[lid]=1;
				pthread_mutex_unlock(&mutex);
				break;
			}
			pthread_mutex_unlock(&mutex);
		}
	}
	std::cout<<"lock_server::acquire end"<<std::endl;
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab4 code goes here
	std::cout<<"lock_server::release begin lid="<<lid<<std::endl;
	if(lock_table.find(lid)==lock_table.end())
		return lock_protocol::NOENT;
	lock_table[lid]=0;
	std::cout<<"lock_server::release end"<<lid<<std::endl;
  return ret;
}
