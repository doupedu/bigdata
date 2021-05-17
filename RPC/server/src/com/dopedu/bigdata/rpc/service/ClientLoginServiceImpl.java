package com.dopedu.bigdata.rpc.service;

import com.dopedu.bigdata.rpc.protocal.*;

public class ClientLoginServiceImpl implements IClientLoginActionProtocal{
	@Override
	public String login(String username, String pwd) {
		
		return username + "		login succeed£¡";
	}

}
