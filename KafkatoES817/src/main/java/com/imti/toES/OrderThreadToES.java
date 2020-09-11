package com.imti.toES;

import java.io.IOException;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;

public class OrderThreadToES implements Runnable{
	
	private String order;
	private String endPoint;
	private RestClient restClient;
	
	
	
	
	public OrderThreadToES(String order, String endPoint, RestClient restClient) {
		this.order = order;
		this.endPoint = endPoint;
		this.restClient = restClient;
	}




	public void run() {
		Request request = new Request("post", endPoint);
		request.setJsonEntity(order);
		try {
			restClient.performRequest(request);
			System.out.println(Thread.currentThread().getName()+" "+order+ " successfully!");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	

}
