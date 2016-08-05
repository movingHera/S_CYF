package cgz.sy.cyf.yc1;

public class 類別_歸約者 extends com.aliyun.odps.mapred.ReducerBase
{	
	private com.aliyun.odps.data.Record 結果;
	
	public void setup(TaskContext 上下文) throws java.io.IOException
	{
		結果 = 上下文.createOutputRecord();
	}
	  
	public void reduce(com.aliyun.odps.data.Record 鍵, java.util.Iterator<com.aliyun.odps.data.Record> 迭代_值, TaskContext 上下文) throws java.io.IOException
	{
		結果.setBigint(0, 鍵.getBigint("item_id"));
		java.util.Vector<類別_候選商品> 向量_候選商品 = new java.util.Vector<類別_候選商品>();
    	
		while(迭代_值.hasNext())
		{
			com.aliyun.odps.data.Record 值 = 迭代_值.next();
			long 候選商品標識 = 值.getBigint("hxitem_id");
			double 候選商品搭配度 = 值.getDouble("hxitem_dpd");
			long 候選商品套參數 = 值.getBigint("hxitem_tcs");
    		
			向量_候選商品.add(new 類別_候選商品(候選商品標識, 候選商品搭配度, 候選商品套參數));
		}
    	
		java.util.Collections.sort(向量_候選商品);
    	
		for(int 甲 = 0; 甲 < 向量_候選商品.size(); 甲++)
		{
			結果.setBigint(1, 向量_候選商品.get(甲).標識);
			結果.setDouble(2, 向量_候選商品.get(甲).搭配度 * 向量_候選商品.get(甲).套參數);
			上下文.write(結果);
		}	
	}
    
	class 類別_候選商品 implements Comparable<類別_候選商品>
	{
		long 標識;
		double 搭配度;
		double 套參數;
		
		類別_候選商品(long 標, double 搭, long 套)
		{
			標識 = 標;
			搭配度 = 搭;
			套參數 = 1 - Math.pow(5, -套);
		}
		
		public int compareTo(類別_候選商品 另)
		{
			return -Double.compare(搭配度 * 套參數, 另.搭配度 * 另.套參數);
		}
	}
}