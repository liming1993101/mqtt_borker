package com.sh.wd.utils;

import org.quartz.DateBuilder.IntervalUnit;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Date;
import java.util.Map;

import static org.quartz.DateBuilder.futureDate;
import static org.quartz.JobBuilder.newJob;

/**
 * Quatrz调度框架的管理类，用于添加，删除，重置任务
 * 
 * @author zer0
 * @version 1.0
 * @date 2015-11-30
 */
public class QuartzManager {


	
	private static SchedulerFactory sf = new StdSchedulerFactory();
	
	/**
	 * 添加调度任务
	 * @param jobName
	 * @param jobGroupName
	 * @param triggerName
	 * @param triggerGroupName
	 * @param jobClass
	 * @param time
	 * @param jobParam
	 * @param count
	 * @author zer0
	 * @version 1.0
	 * @date 2015-11-28
	 */
	@SuppressWarnings("unchecked")
	public static void addJob(String jobName, String jobGroupName,
							  String triggerName, String triggerGroupName, Class jobClass,
							  int time, int count, Map<String, Object> jobParam) {
        try {  
            Scheduler sched = sf.getScheduler();  
            JobDetail job = newJob(jobClass).withIdentity(jobName, jobGroupName).build();// 任务名，任务组，任务执行类  
            if (jobParam != null && jobParam.size() > 0) {
				//给job添加参数
            	job.getJobDataMap().putAll(jobParam);
			}
            // 触发器  
            Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerName, triggerGroupName)
    	            .startAt(futureDate(time, IntervalUnit.SECOND))
    	            .withSchedule(SimpleScheduleBuilder
    	            		.simpleSchedule()
    	            		.withIntervalInSeconds(time)
    	            		.withRepeatCount(count))
    	            .build();
            Date ft = sched.scheduleJob(job, trigger);

            // 启动
            if (!sched.isShutdown()) {  
                sched.start();  
            }  
        } catch (RuntimeException e){
			throw new RuntimeException(e);
		} catch (Exception e) {
            throw new RuntimeException(e);
        }
	}
	
	/**
	 * 删除调度任务
	 * @param jobName
	 * @param jobGroupName
	 * @param triggerName
	 * @param triggerGroupName
	 * @author zer0
	 * @version 1.0
	 * @date 2015-11-28
	 */
	public static void removeJob(String jobName, String jobGroupName,
								 String triggerName, String triggerGroupName){
		try {
			Scheduler sched = sf.getScheduler();
			TriggerKey triggerKey = new TriggerKey(triggerName, triggerGroupName);
//			JobKey jobKey = new JobKey(jobName, jobGroupName);
			sched.pauseTrigger(triggerKey);//停止触发器  
			sched.unscheduleJob(triggerKey);//移除触发器
			sched.deleteJob(JobKey.jobKey(jobName, jobGroupName));//删除任务
			System.out.println("删除任务成功!");
		} catch (SchedulerException e) {
			System.out.println("删除任务失败!!");
			e.printStackTrace();
		}
	}
	
	/**
	 * 重置调度任务
	 * @param jobName
	 * @param jobGroupName
	 * @param triggerName
	 * @param triggerGroupName
	 * @author zer0
	 * @version 1.0
	 * @date 2015-12-3
	 */
	public static void resetJob(String jobName, String jobGroupName,
								String triggerName, String triggerGroupName, Class jobClass,
								int time, int count, Map<String, Object> jobParam){
		removeJob(jobName, jobGroupName, triggerName, triggerGroupName);
		addJob(jobName, jobGroupName, triggerName, triggerGroupName, jobClass, time, count, jobParam);
	}
 
	
}
