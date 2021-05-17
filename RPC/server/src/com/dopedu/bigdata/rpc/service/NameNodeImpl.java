package com.dopedu.bigdata.rpc.service;
import com.dopedu.bigdata.rpc.protocal.*;

public class NameNodeImpl implements IClientToNameNodeProtocal{
	public String getMetaData(String path) {
		return path + "(block1,block2,...)";
	}

}
