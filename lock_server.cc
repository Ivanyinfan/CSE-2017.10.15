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
	pthread_mutex_lock(&mutex);
	while(lock_table[lid]==1)
	{
		pthread_mutex_unlock(&mutex);
		pthread_mutex_lock(&mutex);
	}
	lock_table[lid]=1;
	pthread_mutex_unlock(&mutex);
	std::cout<<"lock_server::acquire end"<<std::endl;
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab4 code goes here
	std::cout<<"lock_server::release begin lid="<<lid<<std::endl;
	pthread_mutex_lock(&mutex);
	if(lock_table.find(lid)==lock_table.end())
		ret=lock_protocol::NOENT;
	else
		lock_table[lid]=0;
	pthread_mutex_unlock(&mutex);
	std::cout<<"lock_server::release end"<<std::endl;
  return ret;
}
