package hdfsUtil;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.zookeeper.common.IOUtils;
import org.junit.Before;
import org.junit.Test;


public class HdfsUtil {
	FileSystem dfs =null;
	
	/**
	 * 初始化
	 * @throws Exception
	 */
	@Before
	public void init() throws Exception{
		
		Configuration conf = new Configuration();
		dfs = FileSystem.get(new URI("hdfs://192.168.1.60:9000"),conf,"root");
	}
	
	/**
	 * 下载文件
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test 
	public void HdfsDownload() throws IllegalArgumentException, IOException{
		//文件下载
		dfs.copyToLocalFile(false, new Path("/access_log"), new Path("d:/"), true);

	}
	
	/**
	 * 上传文件
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test 
	public void HdfsUpload() throws IllegalArgumentException, IOException{
		//文件上传
		dfs.copyFromLocalFile(false, new Path("in/1.txt"), new Path("~/data/merge/1.txt"));
		//兼容windows 
		//dfs.copyToLocalFile(false, new Path("/access_log"), new Path("d:/"), true);
	}
	
	/**
	 * 目录操作
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void HdfsDir() throws IllegalArgumentException, IOException {
		
		dfs.mkdirs(new Path("/testDir"));
		System.out.println("创建了文件夹： /testDir");
		
		boolean existed = dfs.exists(new Path("/testDir"));
		System.out.println("文件夹testDir是否存在:	" + existed);
		
		if (existed){
			dfs.copyFromLocalFile(new Path("E:/逗皮教育/大数据/software/hadoop-2.7.5.tar.gz"), new Path("/testDir"));
			System.out.println("成功上传文件 到 /testDir 目录");
		}
		
		dfs.delete(new Path("/testDir"), true);
		System.out.println("删除目录 /testDir" );
		
		existed = dfs.exists(new Path("/testDir"));
		System.out.println("文件夹 /testDir是否存在" + existed);
		
		dfs.close();
	}
	
	/**
	 * 查看文件和目录状态
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void HdfsFileStatus() throws IllegalArgumentException, IOException {
		
		//所有文件列表
		RemoteIterator<LocatedFileStatus> listFiles = dfs.listFiles(new Path("/"), true);
		while(listFiles.hasNext()) {
			LocatedFileStatus fileStatus = listFiles.next();
			System.out.println(fileStatus.getPath().getName());
		}
		
		//文件状态
		FileStatus[] listStatus = dfs.listStatus(new Path("/"));
		for(FileStatus f:listStatus) {
			if(f.isDirectory())
				System.out.println("d" + "\t" + f.getPath().getName());
			else
				System.out.println(f.getPath().getName());
		}
	}
	
	
	/**
	 * 查看文件block详细信息
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void HdfsFileBlock() throws IllegalArgumentException, IOException {
		
		//文件分块列表
		BlockLocation[] fbLocation = dfs.getFileBlockLocations(new Path("/testDir/hadoop-2.7.5.tar.gz"), 0, 216929574);
		for(BlockLocation fbl:fbLocation) {
			System.out.println(fbl.getOffset());
			//副本存放服务器列表
			String[] listNames = fbl.getNames();
			for(String name:listNames) {
				System.out.println(name);
			}
		}
	}
	
	/**
	 * IO流操作
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test 
	public void HdfsIO() throws IllegalArgumentException, IOException{
	
		//io流操作 下载
		FSDataInputStream inDownload = dfs.open(new Path("/xxx"));
		//指定偏移量读取,在分布式系统中,可以将数据分片来分配给不同的节点处理
		//in.seek(10);
		FileOutputStream outDownload = new FileOutputStream("d:/yyy");		
		IOUtils.copyBytes(inDownload, outDownload, 4096);
		IOUtils.closeStream(inDownload);
		IOUtils.closeStream(outDownload);
		
		//io流操作 上传
		FileInputStream inUpload = new FileInputStream("d:/xxx1");
		FSDataOutputStream outUpload = dfs.create(new Path("/yyy"));		
		IOUtils.copyBytes(inUpload, outUpload, 4096);
		IOUtils.closeStream(inUpload);
		IOUtils.closeStream(outUpload);
	}
	
	public static void main(String[] args) throws Exception //IOException, Throwable, URISyntaxException
	{
		//创建配置参数的对象
		Configuration conf = new Configuration();
		
		//用户权限可以设置 VM arguments : -DHADOOP_USER_NAME=root
		//conf.set("fs.defaultFS", "hdfs://192.168.1.60:9000"); //hadoop-00
		//创建 hdfs 客户端
		//FileSystem fs = FileSystem.get(conf);
		//
		
		//添加root用户
		FileSystem dfs = FileSystem.get(new URI("hdfs://192.168.1.60:9000"),conf,"root");
		
		//上传文件
		dfs.copyFromLocalFile(new Path("E:/逗皮教育/大数据/data/access_log"), new Path("/test/"));
		
		dfs.close();
	}

}
