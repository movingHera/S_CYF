package cgz.sy.cyf.yc1_yb;

public class 類別_歸約者 extends com.aliyun.odps.mapred.ReducerBase
{	
	private com.aliyun.odps.data.Record 結果;
	
	public void setup(TaskContext 上下文) throws java.io.IOException
	{
		結果 = context.createOutputRecord();
	}
	  
    public void reduce(com.aliyun.odps.data.Record 鍵, java.util.Iterator<com.aliyun.odps.data.Record> 迭代_值, TaskContext 上下文) throws java.io.IOException
    {
   		結果.setBigint("item_id", 鍵.getBigint("item_id"));
    	
    	java.util.HashMap<String, Double> 雜湊圖_詞權 = new java.util.HashMap<String, Double>();
		java.util.HashMap<String, Double> 雜湊圖_候選商品搭配度 = new java.util.HashMap<String, Double>();
    	java.util.Vector<類別_候選商品> 向量_候選商品 = new java.util.Vector<類別_候選商品>();
       	String 詞權字串 = 鍵.getString("termms");
    	if(詞權字串.equals(""))
    		return;
    	
    	String[] 陣列_詞權 = 詞權字串.split(",");
    	double 詞進值長 = 0;
    	for(String 詞_詞權 : 陣列_詞權)
    	{
    		String[] 陣列_詞_詞權 = 詞_詞權.split(":");
    		double 詞進值 = 1 / Math.log(Double.parseDouble(陣列_詞_詞權[1]) * 800000 + 32);
    		詞進值長 += 詞進值 * 詞進值;  		
    		雜湊圖_詞權.put(陣列_詞_詞權[0], 詞進值);
    	}
    	
    	while(迭代_值.hasNext())
    	{
    		com.aliyun.odps.data.Record 值 = 迭代_值.next();
    		String 同類商品詞權字串 = 值.getString("tlitem_termms");
    		String 同類商品搭配商品字串 = 值.getString("tlitem_dpitems");

    		if(同類商品詞權字串.equals("") || 同類商品搭配商品字串.equals(""))
    			continue;
    		
    		String[] 陣列_同類商品詞權 = 同類商品詞權字串.split(",");

    		double 詞進值內積 = 0;
    		java.util.HashSet<String> 雜湊集_同類商品_詞 = new java.util.HashSet<String>();
        	for(String 詞_詞權 : 陣列_同類商品詞權)
        	{
        		String[] 陣列_詞_詞權 = 詞_詞權.split(":");
        		if(雜湊集_同類商品_詞.contains(陣列_詞_詞權[0]))
        			continue;
        		雜湊集_同類商品_詞.add(陣列_詞_詞權[0]);
        		
        		double 詞進值 = 1 / Math.log(Double.parseDouble(陣列_詞_詞權[1]) * 800000 + 32);

        		if(雜湊圖_詞權.containsKey(陣列_詞_詞權[0]))
    				詞進值內積 += 雜湊圖_詞權.get(陣列_詞_詞權[0]) * 詞進值;
        	}

        	if(詞進值內積 == 0)
        		continue;

        	String[] 陣列_同列商品_搭配商品 = 同列商品_搭配商品字串.split(",");
        	double 搭配度 = 詞進值內積 / 詞進值長 / Math.sqrt((double)雜湊集_同類商品_詞.size() / 雜湊圖_詞權.size());
        	    		
    		for(String 商品標識 : 陣列_同列商品_搭配商品)
    		{
    			if(!雜湊圖_候選商品搭配度.containsKey(商品標識))
    				雜湊圖_候選商品搭配度.put(商品標識, Math.pow(搭配度 / (1 + 0.15 * Math.log(陣列_同列商品_搭配商品.length)), 4));
    			else
    				雜湊圖_候選商品搭配度.put(商品標識, Math.pow(搭配度 / (1 + 0.15 * Math.log(陣列_同列商品_搭配商品.length)), 4) + 雜湊圖_候選商品搭配度.get(商品標識));
    		}
    	}

    	for(String 商品標識 : 雜湊圖_候選商品搭配度.keySet())
    		向量_候選商品.add(new 類別_候選商品(Long.parseLong(商品標識), 雜湊圖_候選商品搭配度.get(商品標識)));


    	java.util.Collections.sort(向量_候選商品);

        if(向量_候選商品.size() == 0)
            return;

        for(int 甲 = 0; 甲 < 向量_候選商品.size() && 甲 < 500; 甲++)
        {
        	結果.setBigint("hxitem_id", 向量_候選商品.get(甲).標識);
        	結果.setDouble("hxitem_dpd", Math.pow(向量_候選商品.get(甲).搭配度, 0.25));
            上下文.write(結果);      
        }
    }
    
    class 類別_候選商品 implements Comparable<類別_候選商品>
	{
		long 標識;
		double 搭配度;
		
		類別_候選商品(long 標, double 搭)
		{
			標識 = 標;
			搭配度 = 搭;
		}
		
		public int compareTo(類別_候選商品 另)
		{
			return -Double.compare(搭配度, 另.搭配度);
		}
	}
}
