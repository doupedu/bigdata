package com.dopedu.bigdata.loganalyse;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

public class SitesCount {

	public static void main(String[] args) throws Exception {
		
		HashMap<String,Integer> logmap = new HashMap<String,Integer>();
		String line = null;
		String ip = null;
		Integer counts = -1;
		
		BufferedReader br = new BufferedReader(new FileReader("E:/Hadoop/access_log"));
		BufferedWriter outtext = new BufferedWriter(new FileWriter("E:/Hadoop/out_log"));
		
		while(null != (line = br.readLine())) {
			CharSequence s = "member_zhuce_gxzc.php";
			if(line.contains(s))
			{
				String[] fields = line.split(" ");
				if(fields.length < 5) break;
				ip = fields[0];
	
				if(logmap.get(ip) == null) 
					logmap.put(ip,0);
				else
				{
					counts = logmap.get(ip) + 1;
					logmap.put(ip,counts);
				}
			}
		}
		
		System.out.println(logmap.size());
		
		Iterator<Map.Entry<String, Integer>> it = logmap.entrySet().iterator();
		while(it.hasNext())
		{
			outtext.write(it.next() + "\n");
		}
			
		br.close();
		outtext.close();
	}

}
