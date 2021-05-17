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
	 * ��ʼ��
	 * @throws Exception
	 */
	@Before
	public void init() throws Exception{
		
		Configuration conf = new Configuration();
		dfs = FileSystem.get(new URI("hdfs://192.168.1.60:9000"),conf,"root");
	}
	
	/**
	 * �����ļ�
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test 
	public void HdfsDownload() throws IllegalArgumentException, IOException{
		//�ļ�����
		dfs.copyToLocalFile(false, new Path("/access_log"), new Path("d:/"), true);

	}
	
	/**
	 * �ϴ��ļ�
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test 
	public void HdfsUpload() throws IllegalArgumentException, IOException{
		//�ļ��ϴ�
		dfs.copyFromLocalFile(false, new Path("in/1.txt"), new Path("~/data/merge/1.txt"));
		//����windows 
		//dfs.copyToLocalFile(false, new Path("/access_log"), new Path("d:/"), true);
	}
	
	/**
	 * Ŀ¼����
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void HdfsDir() throws IllegalArgumentException, IOException {
		
		dfs.mkdirs(new Path("/testDir"));
		System.out.println("�������ļ��У� /testDir");
		
		boolean existed = dfs.exists(new Path("/testDir"));
		System.out.println("�ļ���testDir�Ƿ����:	" + existed);
		
		if (existed){
			dfs.copyFromLocalFile(new Path("E:/��Ƥ����/������/software/hadoop-2.7.5.tar.gz"), new Path("/testDir"));
			System.out.println("�ɹ��ϴ��ļ� �� /testDir Ŀ¼");
		}
		
		dfs.delete(new Path("/testDir"), true);
		System.out.println("ɾ��Ŀ¼ /testDir" );
		
		existed = dfs.exists(new Path("/testDir"));
		System.out.println("�ļ��� /testDir�Ƿ����" + existed);
		
		dfs.close();
	}
	
	/**
	 * �鿴�ļ���Ŀ¼״̬
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void HdfsFileStatus() throws IllegalArgumentException, IOException {
		
		//�����ļ��б�
		RemoteIterator<LocatedFileStatus> listFiles = dfs.listFiles(new Path("/"), true);
		while(listFiles.hasNext()) {
			LocatedFileStatus fileStatus = listFiles.next();
			System.out.println(fileStatus.getPath().getName());
		}
		
		//�ļ�״̬
		FileStatus[] listStatus = dfs.listStatus(new Path("/"));
		for(FileStatus f:listStatus) {
			if(f.isDirectory())
				System.out.println("d" + "\t" + f.getPath().getName());
			else
				System.out.println(f.getPath().getName());
		}
	}
	
	
	/**
	 * �鿴�ļ�block��ϸ��Ϣ
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void HdfsFileBlock() throws IllegalArgumentException, IOException {
		
		//�ļ��ֿ��б�
		BlockLocation[] fbLocation = dfs.getFileBlockLocations(new Path("/testDir/hadoop-2.7.5.tar.gz"), 0, 216929574);
		for(BlockLocation fbl:fbLocation) {
			System.out.println(fbl.getOffset());
			//������ŷ������б�
			String[] listNames = fbl.getNames();
			for(String name:listNames) {
				System.out.println(name);
			}
		}
	}
	
	/**
	 * IO������
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test 
	public void HdfsIO() throws IllegalArgumentException, IOException{
	
		//io������ ����
		FSDataInputStream inDownload = dfs.open(new Path("/xxx"));
		//ָ��ƫ������ȡ,�ڷֲ�ʽϵͳ��,���Խ����ݷ�Ƭ���������ͬ�Ľڵ㴦��
		//in.seek(10);
		FileOutputStream outDownload = new FileOutputStream("d:/yyy");		
		IOUtils.copyBytes(inDownload, outDownload, 4096);
		IOUtils.closeStream(inDownload);
		IOUtils.closeStream(outDownload);
		
		//io������ �ϴ�
		FileInputStream inUpload = new FileInputStream("d:/xxx1");
		FSDataOutputStream outUpload = dfs.create(new Path("/yyy"));		
		IOUtils.copyBytes(inUpload, outUpload, 4096);
		IOUtils.closeStream(inUpload);
		IOUtils.closeStream(outUpload);
	}
	
	public static void main(String[] args) throws Exception //IOException, Throwable, URISyntaxException
	{
		//�������ò����Ķ���
		Configuration conf = new Configuration();
		
		//�û�Ȩ�޿������� VM arguments : -DHADOOP_USER_NAME=root
		//conf.set("fs.defaultFS", "hdfs://192.168.1.60:9000"); //hadoop-00
		//���� hdfs �ͻ���
		//FileSystem fs = FileSystem.get(conf);
		//
		
		//���root�û�
		FileSystem dfs = FileSystem.get(new URI("hdfs://192.168.1.60:9000"),conf,"root");
		
		//�ϴ��ļ�
		dfs.copyFromLocalFile(new Path("E:/��Ƥ����/������/data/access_log"), new Path("/test/"));
		
		dfs.close();
	}

}
