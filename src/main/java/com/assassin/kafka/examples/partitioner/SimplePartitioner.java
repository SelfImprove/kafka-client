package com.assassin.kafka.examples.partitioner;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * kafka生产端消息分区
 * @author assassin
 * @date 2017年12月14日
 */
public class SimplePartitioner implements Partitioner {

	private Logger logger = LoggerFactory.getLogger(SimplePartitioner.class);
	
	@Override
	public void configure(Map<String, ?> configs) {
		logger.debug("configure: " + configs);
	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		// 获取分区个数
		int partitionCount = cluster.partitionCountForTopic(topic);
		// 处理key
		int intKey = 0;
		try {
			intKey = Integer.parseInt(key.toString());
		} catch (Exception e){
			intKey = key == null ? 0 :key.hashCode();
		}
		int partitonNum = intKey % partitionCount;
		
		logger.debug("partition topic:" + topic + ", key:" + key + ", partitonCount:" + partitionCount + ", partitionNum:" + partitonNum);
		return partitonNum;
	}

	@Override
	public void close() {
		logger.debug("close!");
	}

}
