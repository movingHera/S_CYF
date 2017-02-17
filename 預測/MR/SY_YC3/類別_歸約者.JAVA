package cgz.sy.cyf.yc3;

public class 類別_歸約者 extends com.aliyun.odps.mapred.ReducerBase
{	
	private com.aliyun.odps.data.Record 結果;
	
	public void setup(TaskContext 上下文) throws java.io.IOException
	{
		結果 = 上下文.createOutputRecord();
	}
	  
	public void reduce(com.aliyun.odps.data.Record 鍵, java.util.Iterator<com.aliyun.odps.data.Record> 迭代_值, TaskContext 上下文) throws java.io.IOException
	{
		結果.setBigint("item_id", 鍵.getBigint("item_id"));
		String 詞權字串 = 鍵.getString("termms");
    	
		java.util.HashMap<String, Double> 雜湊圖_詞權 = new java.util.HashMap<String, Double>();
		java.util.Vector<類別_候選商品> 向量_候選商品 = new java.util.Vector<類別_候選商品>();
		if(詞權字串.equals(""))
			return;
    	
		String[] 數組_詞權 = 詞權字串.split(",");
		double 詞進值長 = 0;
		for(String 詞_詞權 : 數組_詞權)
		{
			String[] 數組_詞_詞權 = 詞_詞權.split(":");
			double 詞進值 = 1 / Math.log(Double.parseDouble(數組_詞_詞權[1]) * 800000 + 32);		
			詞進值長 += 詞進值 * 詞進值;
			雜湊圖_詞權.put(數組_詞_詞權[0], 詞進值);
		}
    	
		while(迭代_值.hasNext())
		{
			com.aliyun.odps.data.Record 值 = 迭代_值.next();
			long 候選商品標識 = 值.getBigint("hxitem_id");
			String 候選商品詞權字串 = 值.getString("hxitem_terms");
    		
			if(候選商品詞權字串.equals(""))
				continue;
    		
			String[] 數組_候選商品詞權 = 候選商品詞權字串.split(",");

			double 詞進值內積 = 0;
			java.util.HashSet<String> 雜湊集_候選商品_標題分詞 = new java.util.HashSet<String>();
			for(String 詞_詞權 : 數組_候選商品詞權)
			{
				String[] 數組_詞_詞權 = 詞_詞權.split(":");
				if(雜湊集_候選商品_標題分詞.contains(數組_詞_詞權[0]))
					continue;

				雜湊集_候選商品_標題分詞.add(數組_詞_詞權[0]);
        		
				double 詞進值 = 1 / Math.log(Double.parseDouble(數組_詞_詞權[1]) * 800000 + 32);

				if(雜湊圖_詞權.containsKey(數組_詞_詞權[0]))       			
					詞進值內積 += 雜湊圖_詞權.get(數組_詞_詞權[0]) * 詞進值;
			}
        	
			if(詞進值內積 == 0)
				continue;

			double 搭配度 = 詞進值內積 / 詞進值長 / Math.sqrt((double)雜湊集_候選商品_標題分詞.size() / 雜湊圖_詞權.size());
			向量_候選商品.add(new 類別_候選商品(候選商品標識, 搭配度));
		}
    	
		java.util.Collections.sort(向量_候選商品);
    	
		for(int 甲 = 0; 甲 < 向量_候選商品.size() && 甲 < 500; 甲++)
		{
			結果.setBigint("hxitem_id", 向量_候選商品.get(甲).標識);
			結果.setDouble("hxitem_dpd", 向量_候選商品.get(甲).搭配度);
			上下文.write(結果);
		}	
	}
    
	class 類別_候選商品 implements Comparable<類別_候選商品>
	{
		long 標識;
		double 搭配度;
		
		類別_候選商品(long 身, double 搭)
		{
			標識 = 身;
			搭配度 = 搭;
		}
		
		public int compareTo(類別_候選商品 另)
		{
			return -Double.compare(搭配度, 另.搭配度);
		}
	}
}
