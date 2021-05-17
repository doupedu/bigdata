package fileMerge;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

class MyPathFilter implements PathFilter{
	String reg = null;
	MyPathFilter(String reg){
		this.reg = reg;
	}
	@Override
	public boolean accept(Path path) {
		if(!path.toString().matches(reg))
			return true;
		return false;
	}
}

public class Merge {
	Path inPath = null;
	Path outPath = null;
	FileSystem dfsIn = null;
	FileSystem dfsOut = null;
	
	public Merge(String in,String out) {
		this.inPath = new Path(in);
		this.outPath = new Path(out);
	}
	
	public void init() throws Exception{
		
		Configuration conf = new Configuration();
		dfsIn = FileSystem.get(new URI(inPath.toString()),conf,"hadoop");//hadoop-00
		dfsOut = FileSystem.get(new URI(outPath.toString()),conf,"hadoop");
	}

	public void doMerge() throws IOException{
		//file status
		FileStatus[] listStatus = dfsIn.listStatus(inPath, new MyPathFilter(".*\\.log"));
		//output stream
		FSDataOutputStream dst = dfsOut.create(outPath);
		
		for(FileStatus st:listStatus) {
			System.out.println("Path:	" + st.getPath() + 
					"	size:	" + st.getLen() + 
					" 	privilege:	"  + st.getPermission() +
					"	conetent:	"  );
			//input stream
			FSDataInputStream fread = dfsIn.open(st.getPath()); 
			byte[] data = new byte[1024];
			int num = -1;
			while((num = fread.read(data)) > 0) {
				dst.write(data,0,num);
			}
			fread.close();
		}
		
		dst.close();	
	}
	/*
	 *merge those files in /in directory exclude files with extended name as ".log"
	 *results output to /out/merge.txt
	 */
	public static void main(String[] args) throws Exception{
		Merge merge = new Merge(
				"hdfs://192.168.1.60:9000/in/",
				"hdfs://192.168.1.60:9000/out/merge.txt");
		merge.init();
		merge.doMerge();
	}
	
	
	

}
