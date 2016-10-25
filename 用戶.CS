namespace 純規則.桑楡.今日頭條競賽
{
	public class 類別_用戶
	{
		public System.String 標識 { get; }
		public System.Collections.Generic.HashSet<System.Int32> 標籤雜湊集 { get; }
		public System.Collections.Generic.HashSet<System.Int32> 描述詞語雜湊集 { get; }
		public System.Collections.Generic.HashSet<System.Int32> 描述字元雜湊集 { get; }
		public System.Collections.Generic.Dictionary<System.String, System.Double> 問題標識字典 { get; }

		#region 附加屬性
		public System.Double 訓練集被邀請數 = 0;
		public System.Double 訓練集回答數 = 0;
		public System.Collections.Generic.Dictionary<System.Int32, System.Double> 訓練集按問題標籤被邀請數 = new System.Collections.Generic.Dictionary<System.Int32, System.Double>();
		public System.Collections.Generic.Dictionary<System.Int32, System.Double> 訓練集按問題標籤回答數 = new System.Collections.Generic.Dictionary<System.Int32, System.Double>();
		#endregion

		public 類別_用戶(System.String 標識, System.String 標籤字串, System.String 描述詞語字串, System.String 描述字元字串)
		{
			this.標識 = 標識;

			標籤雜湊集 = new System.Collections.Generic.HashSet<System.Int32>();

			var 標籤字串分詞 = 標籤字串.Split('/');
			foreach (var 標籤 in 標籤字串分詞)
				標籤雜湊集.Add(System.Int32.Parse(標籤));

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
				foreach (var 描述字元 in 標籤字串分詞)
					描述字元雜湊集.Add(System.Int32.Parse(描述字元));
			}

			問題標識字典 = new System.Collections.Generic.Dictionary<System.String, System.Double>();
		}

		public 類別_用戶(類別_用戶 用戶)
		{
			標識 = 用戶.標識;

			標籤雜湊集 = new System.Collections.Generic.HashSet<System.Int32>();
			foreach (var 標籤 in 用戶.標籤雜湊集)
				標籤雜湊集.Add(標籤);

			描述詞語雜湊集 = new System.Collections.Generic.HashSet<System.Int32>();
			foreach (var 描述詞語 in 用戶.描述詞語雜湊集)
				描述詞語雜湊集.Add(描述詞語);

			描述字元雜湊集 = new System.Collections.Generic.HashSet<System.Int32>();
			foreach (var 描述字元 in 用戶.描述字元雜湊集)
				描述字元雜湊集.Add(描述字元);

			問題標識字典 = new System.Collections.Generic.Dictionary<System.String, System.Double>();
		}
	}
}
