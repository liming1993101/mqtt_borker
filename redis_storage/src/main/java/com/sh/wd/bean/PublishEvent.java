package com.sh.wd.bean;

import io.netty.handler.codec.mqtt.MqttQoS;

import java.io.Serializable;

/**
 *  发送消息的事件类，把协议的处理当做事件来进行就可以很好的进行封装
 * 
 * @author zer0
 * @version 1.0
 * @date 2015-05-11
 */
public class PublishEvent implements Serializable {
	String topic;
	MqttQoS qos;
	byte[] message;
	boolean retain;
	String clientID;
	//针对Qos1和Qos2
	int packgeID;
	
	public PublishEvent(String topic, MqttQoS qos, byte[] message, boolean retain, String clientID, Integer pkgID){
		this.topic = topic;
		this.qos = qos;
		this.message = message;
		this.retain = retain;
		this.clientID = clientID;
		if (qos != MqttQoS.AT_MOST_ONCE) {
			this.packgeID = pkgID;
		}
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public MqttQoS getQos() {
		return qos;
	}

	public void setQos(MqttQoS qos) {
		this.qos = qos;
	}

	public byte[] getMessage() {
		return message;
	}

	public void setMessage(byte[] message) {
		this.message = message;
	}

	public boolean isRetain() {
		return retain;
	}

	public void setRetain(boolean retain) {
		this.retain = retain;
	}

	public String getClientID() {
		return clientID;
	}

	public void setClientID(String clientID) {
		this.clientID = clientID;
	}

	public int getPackgeID() {
		return packgeID;
	}

	public void setPackgeID(int packgeID) {
		this.packgeID = packgeID;
	}

}
