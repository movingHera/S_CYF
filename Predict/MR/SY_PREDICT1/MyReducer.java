package cgz.sy.cyf.yc1;

import com.aliyun.odps.mapred;
import com.aliyun.odps.data;
import java.io;
import java.util;

public class MyMapper extends ReducerBase
{	
	private Record result;
	
	public void setup(TaskContext context) throws IOException
	{
		result = context.createOutputRecord();
	}
	  
	public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException
	{
		result.setBigint("item_id", key.getBigint("item_id"));
		java.util.Vector<HxItem> hxitems = new Vector<HxItem>();
    	
		while(values.hasNext())
		{
			Record val = values.next();
			long hxitem_id = val.getBigint("hxitem_id");
			double hxitem_dpd = val.getDouble("hxitem_dpd");
			long hxitem_tcs = val.getBigint("hxitem_tcs");
    		
			hxitems.add(new HxItem(hxitem_id, hxitem_dpd, hxitem_tcs));
		}
    	
		Collections.sort(hxitems);
    	
		for(int i = 0; i < hxitems.size(); i++)
		{
			result.setBigint("hxitem_id", hxitems.get(i).id);
			result.setDouble("hxitem_dpd", hxitems.get(i).dpd * hxitems.get(i).tcs);
			context.write(result);
		}	
	}
    
	class HxItem implements Comparable<HxItem>
	{
		long id;
		double dpd;
		double tcs;
		
		HxItem(long i, double d, long t)
		{
			id = i;
			dpd = d;
			tcs = 1 - Math.pow(5, -t);
		}
		
		public int compareTo(HxItem a)
		{
			return -Double.compare(dpd * tcs, a.dpd * a.tcs);
		}
	}
}
