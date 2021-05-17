package com.dopedu.bigdata.rpc.service;
import com.dopedu.bigdata.rpc.protocal.*;

import java.io.IOException;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import org.apache.hadoop.ipc.RPC.Server;


public class LaunchService {
	public  static void main(String[] args) throws Exception, IOException {
		Builder builder = new Builder(new Configuration());
		builder.setBindAddress("localhost")
			   .setPort(9001)
			   .setProtocol(IClientToNameNodeProtocal.class)
			   .setInstance(new NameNodeImpl());
		
		Server server = builder.build();
		server.start();

		Builder builder1 = new Builder(new Configuration());
		builder1.setBindAddress("localhost")
			   .setPort(9002)
			   .setProtocol(IClientLoginActionProtocal.class)
			   .setInstance(new ClientLoginServiceImpl());
		
		Server server1 = builder1.build();
		server1.start();
	
	}
}
