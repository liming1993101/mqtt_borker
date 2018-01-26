package com.sh.wd.server.job;

import com.sh.wd.spi.impl.ProtocolProcessor;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 *  Publish消息重发事件需要做的工作，即重发消息到对应的clientID
 * 
 * @author zer0
 * @version 1.0
 * @date 2015-11-26
 */
public class RePubRelJob implements Job{
	
	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
		//取出参数，参数为ProtocolProcess，调用此类的函数
		JobDataMap dataMap = jobExecutionContext.getJobDetail().getJobDataMap();
		ProtocolProcessor process = (ProtocolProcessor) dataMap.get("ProtocolProcessor");
		String pubRelKey = (String) dataMap.get("pubRelKey");
//		process.reUnKnowPubRelMessage(pubRelKey);
	}
	
}
