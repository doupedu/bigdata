package com.dopedu.bigdata.rpc.protocal;

public interface IClientLoginActionProtocal {
	public static final long versionID = 2L;
	public String login(String user, String pwd);
}
