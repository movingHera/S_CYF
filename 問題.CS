namespace 純規則.桑楡.今日頭條競賽
{
	public class 類別_問題
	{
		public System.String 標識;
		public System.Int32 標籤;
		public System.Collections.Generic.HashSet<System.Int32> 描述詞語雜湊集;
		public System.Collections.Generic.HashSet<System.Int32> 描述字元雜湊集;
		public System.Collections.Generic.Dictionary<System.String, System.Double> 用戶標識字典 { get; }
		public System.Int32 點讚數;
		public System.Int32 回答數;
		public System.Int32 精品回答數;

		#region 附加屬性
		public System.Double 訓練集邀請數 = 0;
		public System.Double 訓練集回答數 = 0;
		#endregion

		public 類別_問題(System.String 標識, System.Int32 標籤, System.String 描述詞語字串, System.String 描述字元字串, System.Int32 點, System.Int32 答, System.Int32 精答)
		{
			this.標識 = 標識;
			this.標籤 = 標籤;

			描述詞語雜湊集 = new System.Collections.Generic.HashSet<System.Int32>();
			if (描述詞語字串 != "/")
			{
				var 描述詞語字串分詞 = 描述詞語字串.Split('/');
				foreach (var 描述詞語 in 描述詞語字串分詞)
					描述詞語雜湊集.Add(System.Int32.Parse(描述詞語));
			}

			描述字元雜湊集 = new System.Collections.Generic.HashSet<System.Int32>();
			if (描述字元字串 != "/")
			{
				var 描述字元字串分詞 = 描述詞語字串.Split('/');
				foreach (var 描述字元 in 描述字元字串分詞)
					描述字元雜湊集.Add(System.Int32.Parse(描述字元));
			}

			用戶標識字典 = new System.Collections.Generic.Dictionary<System.String, System.Double>();
			點讚數 = 點;
			回答數 = 答;
			精品回答數 = 精答;
		}

		public 類別_問題(類別_問題 問題)
		{
			標識 = 問題.標識;
			標籤 = 問題.標籤;

			描述詞語雜湊集 = new System.Collections.Generic.HashSet<System.Int32>();
			foreach (var 描述詞語 in 問題.描述詞語雜湊集)
				描述詞語雜湊集.Add(描述詞語);

			描述字元雜湊集 = new System.Collections.Generic.HashSet<System.Int32>();
			foreach (var 描述字元 in 問題.描述字元雜湊集)
				描述字元雜湊集.Add(描述字元);


			用戶標識字典 = new System.Collections.Generic.Dictionary<System.String, System.Double>();
			點讚數 = 問題.點讚數;
			回答數 = 問題.回答數;
			精品回答數 = 問題.精品回答數;
		}
	}
}
