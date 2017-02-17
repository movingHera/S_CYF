package cgz.sy.cyf.yc1;

public class 類別_映射者 extends com.aliyun.odps.mapred.MapperBase
{
	private com.aliyun.odps.data.Record 鍵, 值;
    
	public void setup(TaskContext 上下文) throws java.io.IOException
	{
		鍵 = 上下文.createMapOutputKeyRecord();
		值 = 上下文.createMapOutputValueRecord();
	}

	public void map(long 記錄數, com.aliyun.odps.data.Record 記錄, TaskContext 上下文) throws java.io.IOException
	{
		鍵.setBigint("item_id", 記錄.getBigint("item_id"));
		鍵.setBigint("hxitem_id", 記錄.getBigint("hxitem_id"));
		值.setDouble("hxitem_dpd", 記錄.getDouble("hxitem_dpd"));
		值.setBigint("hxitem_tcs", 記錄.getBigint("hxitem_tcs"));
		上下文.write(鍵, 值);
	}
}
