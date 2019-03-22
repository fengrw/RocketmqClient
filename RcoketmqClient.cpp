#include "mqredis.h"

#if 0
std::mutex g_rmtx;
std::condition_variable g_rfinished;
TpsReportService g_rtps;
std::list<string> g_rmsglist;
boost::atomic<int> g_msgCount(1);
#endif

CMQResids::CMQResids(string strAdd,int isConsumer,int isPush):m_pushconsumer("traffic_group"),m_consumer("traffic_group"),m_producer("traffic_group")
{
	m_llOffSet = 0;
	m_firstStart = 1;
	if(isConsumer == 1)
	{
		if(isPush == 1)
		{
			m_bPushConsumerFlag = true;
			m_bConsumerFlag = false;
		}else{
			m_bConsumerFlag = true;
			m_bPushConsumerFlag = false;
		}
		m_bProducerFlag = false;
		StartConsumer(strAdd);
	}else{
		StartProducer(strAdd);	
		m_bProducerFlag = true;
		m_bConsumerFlag = false;
	}
}

CMQResids::~CMQResids()
{	
	if(m_bConsumerFlag)
	{
		m_consumer.shutdown();
	}
	if(m_bPushConsumerFlag)
	{
		m_pushconsumer.shutdown();
	}
	if (m_bProducerFlag)
	{
		m_producer.shutdown();
	}
}

void CMQResids::StartConsumer(string strAdd)
{
	if(m_bConsumerFlag == true)
	{
		// 设置MQ的NameServer地址
		printf("[RocketMq]Addr: %s\n", strAdd.c_str());
		m_consumer.setNamesrvAddr(strAdd);
		// 设置消费模式，CLUSTERING-集群模式，BROADCASTING-广播模式
		printf("[RocketMq]Model: %s\n", "BROADCASTING");
		m_consumer.setMessageModel(rocketmq::BROADCASTING);
		// 非阻塞模式，拉取超时时间，默认10s
		//m_consumer.setConsumerPullTimeoutMillis(4000);
		// 长轮询模式，Consumer连接在Broker挂起最长时间，默认20s
		//m_consumer.setBrokerSuspendMaxTimeMillis(3000);
		// 长轮询模式，拉取超时时间，默认30s
		//m_consumer.setConsumerTimeoutMillisWhenSuspend(5000);
		// 启动消费者
		printf("[RocketMq]StartConsumer\n");
		m_consumer.start();
	}else{
		m_pushconsumer.setNamesrvAddr(strAdd);
  		m_pushconsumer.setGroupName("traffic_group");
  		//m_pushconsumer.setNamesrvDomain(info.namesrv_domain);
  		m_pushconsumer.setConsumeFromWhere(rocketmq::CONSUME_FROM_LAST_OFFSET);
		m_pushconsumer.setAsyncPull(false);
		m_pushconsumer.setMessageModel(rocketmq::BROADCASTING);
		//m_pushconsumer.setInstanceName("traffic_group");
		m_pushconsumer.subscribe("TopicTest2", "*");
  		m_pushconsumer.setConsumeThreadCount(15);
  		m_pushconsumer.setTcpTransportTryLockTimeout(1000);
  		m_pushconsumer.setTcpTransportConnectTimeout(400);
		m_pushconsumer.registerMessageListener(&m_msglistener);

		printf("[RocketMq]StartPushConsumer\n");
	 	try{
	 	   m_pushconsumer.start();
	 	} catch (MQClientException &e) {
	 	   cout << e << endl;
	 	}
	 	g_rtps.start();
	}
}

void CMQResids::StartProducer(string strAdd)
{
	// 设置MQ的NameServer地址
	printf("[RocketMq]Addr: %s\n", strAdd.c_str());
	m_producer.setNamesrvAddr(strAdd);
	// 启动消费者
	printf("[RocketMq]StartProducer\n");
	m_producer.start();
}

int CMQResids::GetPushData(std::list<string>& lstrData)
{
	std::unique_lock<std::mutex> lck(g_rmtx);
	g_rfinished.wait(lck);
	std::list<string>::iterator it = g_rmsglist.begin();
	for (; it != g_rmsglist.end(); it++)
	{
		string strmsg = *it;
		lstrData.push_back(strmsg);
	}
	g_rmsglist.clear();
}


long long CMQResids::GetData(string strTopic, std::vector<string>& lstrData)
{
	while(true)
	{
		//获取指定topic的路由
		std::vector<MQMessageQueue> mqs;
		m_consumer.fetchSubscribeMessageQueues(strTopic,mqs);
		std::vector<rocketmq::MQMessageQueue>::iterator it = mqs.begin();
		bool nFirst = true;
		for(; it!=mqs.end(); it++)
		{
			MQMessageQueue mq = *it;
			//if(m_firstStart == 1)
			{
				//读取本地offset
				//m_llOffSet = m_consumer.fetchConsumeOffset(mq,true);
				//m_firstStart = 0;
			//}else{
				//读取内存offset
				m_llOffSet = m_consumer.fetchConsumeOffset(mq,false);
			}
			printf("m_llOffSet:%d\n",m_llOffSet);
			if(m_llOffSet == -1)
				m_llOffSet = 0;
			bool noNewMsg = false;		
			while (!noNewMsg)
			{
				try
				{
					// 拉取消息
					//从指定set开始读取
					PullResult pullResult = m_consumer.pull(mq, "*", m_llOffSet, 32);
					if ((pullResult.pullStatus == FOUND) && (!pullResult.msgFoundList.empty()))
					{
						printf("[RocketMQ]get message %d ,current position %lld ,total position %lld\n", pullResult.msgFoundList.size(), pullResult.nextBeginOffset, m_llOffSet);
						std::vector<rocketmq::MQMessageExt>::iterator it = pullResult.msgFoundList.begin();
						for (;it!=pullResult.msgFoundList.end();it++)
						{
							string strData = (*it).getBody();
							lstrData.push_back(strData);
						}
						//m_llOffSet = (pullResult.nextBeginOffset > m_llOffSet ? pullResult.nextBeginOffset : m_llOffSet);
						m_llOffSet = pullResult.nextBeginOffset;
						//更新进度还用原来的
						m_consumer.updateConsumeOffset(mq, pullResult.nextBeginOffset);
					}
					else
					{
						break;
					}
				}
				catch (MQException& e)
				{
					std::cout<<e<<std::endl;
				}
			}	
		}	
		if (!lstrData.empty())
		{
			return m_llOffSet;
		}
	}
	return m_llOffSet;
}


void CMQResids::SendData(string strTopic, string strTag, string strKey, string strData)
{
	try
	{
		MQMessage msg(strTopic, strTag, strKey, strData);
		// 同步生产消息
		m_producer.send(msg);
	}catch(MQClientException& e)
	{
		printf("[RocketMQ]数据发送失败： [topic]%s[tag]%s[key]%s[info]%s[reason]%s\n", strTopic.c_str(), strTag.c_str(), strKey.c_str(), strData.c_str(), e.what());
	}
}

/*
int main()
{
	string strAdd = "192.168.32.237:9876";
#if 0	
	//consumer
	CMQResids ConsumerOper(strAdd,1,0);
	//producer
	//CMQResids ProducerOper(strAdd,0,0);
	//发送数据Key NOTIFY_DATA 0
	string strTopic = "TopicTest2";
	string strTag = "traffic_tag";
	string strKey = "";
	string strData = "7894561";
	//ProducerOper.SendData(strTopic, strTag, strKey, strData);
	//接收数据
	std::vector<string> lstrData;
	while (true)
	{
		ConsumerOper.GetData(strTopic, lstrData);
		std::vector<string>::iterator it = lstrData.begin();
		for (; it != lstrData.end(); it++)
		{
			cout <<"[Info]" << *it << endl;
		}
		lstrData.clear();
	}
#else
	std::list<string> msgData;
	CMQResids ConsumerOper(strAdd,1,1);
	ConsumerOper.GetPushData(msgData);
	std::list<string>::iterator it = msgData.begin();
	for (; it != msgData.end(); it++)
	{
		cout <<"[Info]" << *it << endl;
	}
	msgData.clear();
#endif
	return 0;
}
*/
