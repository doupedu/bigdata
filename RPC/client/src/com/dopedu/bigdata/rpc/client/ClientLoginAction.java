package com.dopedu.bigdata.rpc.client;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import com.dopedu.bigdata.rpc.protocal.*;

public class ClientLoginAction {
	public static void main(String[] args) throws IOException {
		IClientLoginActionProtocal loginService = RPC.getProxy(IClientLoginActionProtocal.class, 2L, 
				new InetSocketAddress("localhost",9002), new Configuration());
		String loginResult = loginService.login("dopedu","888888");
		System.out.println(loginResult);
	}

}
