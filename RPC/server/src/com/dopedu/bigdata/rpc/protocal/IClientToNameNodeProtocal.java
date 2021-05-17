package com.dopedu.bigdata.rpc.protocal;

public interface IClientToNameNodeProtocal {
	public static final long versionID = 1L;
	public String getMetaData(String path);

}
