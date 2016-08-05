package cgz.sy.cyf.yc4;

public class 類別_歸約者 extends com.aliyun.odps.mapred.ReducerBase
{	
	private com.aliyun.odps.data.Record 結果;
	
	public void setup(TaskContext context) throws java.io.IOException
	{
		結果 = context.createOutputRecord();
	}
    
	public void reduce(com.aliyun.odps.data.Record 鍵, java.util.Iterator<com.aliyun.odps.data.Record> 迭代_值, TaskContext context) throws java.io.IOException
	{
		結果.setBigint(0, 鍵.getBigint("item_id"));
		java.util.HashMap<Long, Long> 雜湊圖_搭配類目 = new java.util.HashMap<Long, Long>();
		java.util.HashMap<Long, Double> 雜湊圖_搭配購買商品 = new java.util.HashMap<Long, Double>();
		java.util.HashMap<Long, Long> 雜湊圖_商品_類目 = new java.util.HashMap<Long, Long>();
		java.util.Vector<類別_搭配購買商品> 向量_搭配購買商品 = new java.util.Vector<類別_搭配購買商品>();
    	
		while(迭代_值.hasNext())
		{
			com.aliyun.odps.data.Record 值 = 迭代_值.next();
 			long 候選商品標識 = 值.getBigint("hxitem_id");
			long 候選商品類目標識 = 值.getBigint("hxitem_cat_id");
			String 搭配類目字串 = 值.getString("dpcats");
			double 標識 = 值.getBigint("bz");
    		    		
			if(標識 == 0)
			{    	
				if(!搭配類目字串.equals(""))
				{
					String[] 數組_搭配類目 = 搭配類目字串.split(",");
					for(String 搭配類目 : 數組_搭配類目)
					{
						String[] 數組_搭配類目標識_權值 = 搭配類目.split(":");
						雜湊圖_搭配類目.put(Long.parseLong(數組_搭配類目標識_權值[0]), Long.parseLong(數組_搭配類目標識_權值[1]));
					}
				}
			}
			else
    			{
				if(雜湊圖_搭配購買商品.containsKey(候選商品標識))
					雜湊圖_搭配購買商品.put(候選商品標識, 雜湊圖_搭配購買商品.get(候選商品標識) + 標識);
				else
					雜湊圖_搭配購買商品.put(候選商品標識, 標識);
    			
				雜湊圖_商品_類目.put(候選商品標識, 候選商品類目標識);
			}
		}
		if(雜湊圖_搭配購買商品.size() == 0)
			return;
			
		for(long 搭配購買商品 : 雜湊圖_搭配購買商品.keySet())		
		{
			if(!雜湊圖_搭配類目.containsKey(雜湊圖_商品_類目.get(搭配購買商品)))
				continue;
				
			向量_搭配購買商品.add(new 類別_搭配購買商品(搭配購買商品, 雜湊圖_搭配購買商品.get(搭配購買商品)));
		}
    	
		java.util.Collections.sort(向量_搭配購買商品);
    	
		for(類別_搭配購買商品 搭配購買商品 : 向量_搭配購買商品)
		{
			結果.setBigint(1, 搭配購買商品.商品標識);
			結果.setDouble(2, 搭配購買商品.搭配度);
			context.write(結果);
		}		
	}
    
	class 類別_搭配購買商品 implements Comparable<類別_搭配購買商品>
	{
		long 商品標識;
		double 搭配度;
   		
		類別_搭配購買商品(long 商, double 搭)
		{
			商品標識 = 商;
			搭配度 = 搭;
		}
   		
		public int compareTo(類別_搭配購買商品 另)
		{
			return -Double.compare(搭配度, 另.搭配度);
		}
	}
}