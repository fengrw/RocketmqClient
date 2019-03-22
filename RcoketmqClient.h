#ifndef _MQ_REDIS_
#define _MQ_REDIS_

#include <iostream>
#include <stdio.h>
#include <list>
#include <mutex>
#include <condition_variable>
#include "DefaultMQPullConsumer.h"
#include "DefaultMQPushConsumer.h"
#include "DefaultMQProducer.h"
#include "common.h"

using namespace rocketmq;
using namespace std;

 #if 0
extern std::mutex g_rmtx;
extern std::condition_variable g_rfinished;
extern TpsReportService g_rtps;
extern std::list<string> g_rmsglist;
#else
std::mutex g_rmtx;
std::condition_variable g_rfinished;
TpsReportService g_rtps;
std::list<string> g_rmsglist;
#endif

class MyMsgListener : public MessageListenerConcurrently {
 public:
  MyMsgListener() {}
  virtual ~MyMsgListener() {}

  virtual ConsumeStatus consumeMessage(const std::vector<MQMessageExt> &msgs) {
    g_msgCount.store(g_msgCount.load() - msgs.size());
	printf("msgs.size:%d\n",msgs.size());
    for (size_t i = 0; i < msgs.size(); ++i) {
      g_rtps.Increment();
      cout << "msg body: "<<  msgs[i].getBody() << endl;
	  string strmsg = msgs[i].getBody();
	  g_rmsglist.push_back(strmsg);
    }

    if (g_msgCount.load() <= 0) {
      std::unique_lock<std::mutex> lck(g_rmtx);
      g_rfinished.notify_one();
    }
    return CONSUME_SUCCESS;
  }
};


class CMQResids
{
public:
	CMQResids(string strAdd,int isConsumer,int isPush);
	~CMQResids();

	//consumer 接收信息
	long long GetData(string strTopic, std::vector<string>& lstrData);
	int GetPushData(std::list<string>& lstrData);
	//发送信息
	void SendData(string strTopic, string strTag, string strKey, string strData);
private:
	void StartConsumer(string strAdd);
	void StartProducer(string strAdd);
private:
	
	DefaultMQPullConsumer m_consumer;
	DefaultMQPushConsumer m_pushconsumer;
	DefaultMQProducer m_producer;
	long long m_llOffSet;
	int  m_firstStart;
	bool m_bConsumerFlag;
	bool m_bPushConsumerFlag;
	bool m_bProducerFlag;
	MyMsgListener m_msglistener;
};

#endif  // _MQ_REDIS_


