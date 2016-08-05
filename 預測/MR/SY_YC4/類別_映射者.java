package cgz.sy.cyf.yc4;

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
		if(上下文.getInputTableInfo().getTableName().equalsIgnoreCase("sy_test_items_dpcats"))
		{
			鍵.setBigint("item_id", 記錄.getBigint("item_id"));
			值.setBigint("hxitem_id", 0L);
			值.setBigint("hxitem_cat_id", 0L);
			值.setString("dpcats", 記錄.getString("dpcats"));
			值.setBigint("bz", 0L);
			上下文.write(鍵, 值);
		}
		else
		{
			值.setString("dpcats", "");
			值.setBigint("bz", 1L);
    		
			String[] 數組_商品購買字串 = 記錄.getString("gmitems").split(",");
			java.util.HashMap<Long, 類別_商品購買> 雜湊團_商品購買_訓練集 = new java.util.HashMap<Long, 類別_商品購買>();
			java.util.Vector<類別_商品購買> 向量_商品購買_測試集 = new java.util.Vector<類別_商品購買>();
	
			for(String 商品購買字串 : 數組_商品購買字串)
			{
				String[] 數組_商品購買 = 商品購買字串.split(":");
				long 商品標識 = Long.parseLong(數組_商品購買[0]);
				long 類目標識 = Long.parseLong(數組_商品購買[1]);
				long 購買日期 = Long.parseLong(數組_商品購買[2]);
	
				if(數組_商品購買[3].equals("1"))
					向量_商品購買_測試集.add(new 類別_商品購買(商品標識, 類目標識, 購買日期));
	    		
				雜湊團_商品購買_訓練集.put(商品標識, new 類別_商品購買(商品標識, 類目標識, 購買日期));
			}
	    	
			for(類別_商品購買 商品購買 : 向量_商品購買_測試集)
			{
				鍵.setBigint("item_id", 商品購買.商品標識);
	    		
				for(Long 候選商品標識 : 雜湊團_商品購買_訓練集.keySet())
				{
					if(候選商品標識 == 商品購買.商品標識)
						continue;
					if(雜湊團_商品購買_訓練集.get(候選商品標識).購買日期 != 商品購買.購買日期)
						continue;
	        			
					值.setBigint("hxitem_id", 候選商品標識);
					值.setBigint("hxitem_cat_id", 雜湊團_商品購買_訓練集.get(候選商品標識).類目標識);
					上下文.write(鍵, 值);
				}
			}
		}
	}
    
	class 類別_商品購買
	{
		long 商品標識;
		long 類目標識;
		long 購買日期;
  		
		類別_商品購買(long 商, long 類, long 購)
		{
			商品標識 = 商;
			類目標識 = 類;
			購買日期 = 購;
		}
	}
}