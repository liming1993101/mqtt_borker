package com.sh.wd.server.job;



import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 *  Publish消息重发事件需要做的工作，即重发消息到对应的clientID
 * 
 * @author zer0
 * @version 1.0
 * @date 2015-11-26
 */
public class RePublishJob implements Job{

	
	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
		//取出参数，参数为ProtocolProcess，调用此类的函数
//		JobDataMap dataMap = jobExecutionContext.getJobDetail().getJobDataMap();
//		Qos1PublishHandler process = (Qos1PublishHandler) dataMap.get("ProtocolProcess");
//		String publishKey = (String) dataMap.get("publishKey");
//     	process.reUnKnowPublishMessage(publishKey);
	}
	
}
