package cgz.sy.cyf.predict1;

import com.aliyun.odps.data;
import com.aliyun.odps.mapred;
import java.io;

public class MyMapper extends MapperBase
{
	private Record key, value;
    
	public void setup(TaskContext context) throws IOException
	{
		key = context.createMapOutputKeyRecord();
		value = context.createMapOutputValueRecord();
	}

	public void map(long recordNum, Record record, TaskContext context) throws IOException
	{
		key.setBigint("item_id", record.getBigint("item_id"));
		key.setBigint("hxitem_id", record.getBigint("hxitem_id"));
		value.setDouble("hxitem_dpd", record.getDouble("hxitem_dpd"));
		value.setBigint("hxitem_tcs", record.getBigint("hxitem_tcs"));
		context.write(key, value);
	}
}
