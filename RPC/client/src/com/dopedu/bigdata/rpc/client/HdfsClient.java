package com.dopedu.bigdata.rpc.client;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import com.dopedu.bigdata.rpc.protocal.IClientToNameNodeProtocal;

public class HdfsClient {
	public static void main(String[] args) throws IOException {
		IClientToNameNodeProtocal loginService = RPC.getProxy(IClientToNameNodeProtocal.class, 1L, 
				new InetSocketAddress("localhost",9001), new Configuration());
		String loginResult = loginService.getMetaData("/test/xxx");
		System.out.println(loginResult);
	}
}
