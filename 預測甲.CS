namespace 純規則.桑楡.今日頭條競賽
{
	public static class 靜態類別_預測甲
	{
		public static System.Collections.Generic.List<類別_邀請> 預測(System.Collections.Generic.Dictionary<System.String, 類別_問題> 問題字典, System.Collections.Generic.Dictionary<System.String, 類別_用戶> 用戶字典, System.Collections.Generic.List<類別_邀請> 訓練集清單, System.Collections.Generic.List<類別_邀請> 測試集清單)
		{
			#region 準備結果清單
			var 預測結果清單 = new System.Collections.Generic.List<類別_邀請>();
			foreach (var 邀請 in 測試集清單)
				預測結果清單.Add(new 類別_邀請(邀請.問題標識, 邀請.用戶標識, -1));
			#endregion

			#region 準備既答字典
			var 既答雜湊集 = new System.Collections.Generic.HashSet<System.String>();

			foreach (var 邀請 in 訓練集清單)
				if (邀請.值 == 1)
					既答雜湊集.Add(邀請.問題標識 + "," + 邀請.用戶標識);

			#endregion

			#region 準備用戶資訊
			foreach (var 邀請 in 訓練集清單)
			{
				問題字典[邀請.問題標識].訓練集邀請數++;
				if (邀請.值 == 1)
					問題字典[邀請.問題標識].訓練集回答數++;
			}

			foreach (var 邀請 in 訓練集清單)
			{
				用戶字典[邀請.用戶標識].訓練集被邀請數++;
				if (邀請.值 == 1)
					用戶字典[邀請.用戶標識].訓練集回答數++;
			}

			#endregion

			#region 準備用戶共答數字典
			var 用戶共答數字典 = new System.Collections.Generic.Dictionary<System.String, System.Double>();
			foreach (var 用戶甲 in 用戶字典)
			{
				foreach (var 問題鍵值對 in 用戶字典[用戶甲.Key].問題標識字典)
				{
					foreach (var 用戶乙鍵值對 in 問題字典[問題鍵值對.Key].用戶標識字典)
					{
						if (用戶乙鍵值對.Key == 用戶甲.Key)
							continue;

						var 用戶甲標識 = 用戶甲.Key; 
						var 用戶乙標識 = 用戶乙鍵值對.Key;
						if (用戶甲標識.CompareTo(用戶乙標識) < 0)
						{
							用戶甲標識 = 用戶乙鍵值對.Key;
							用戶乙標識 = 用戶甲.Key;
						}
						if (!用戶共答數字典.ContainsKey(用戶甲標識 + "," + 用戶乙標識))
							用戶共答數字典[用戶甲標識 + "," + 用戶乙標識] = 0;

						用戶共答數字典[用戶甲標識 + "," + 用戶乙標識]++;
					}
				}
			}
			#endregion

			#region 準備問題權重字典
			var 問題權重字典 = new System.Collections.Generic.Dictionary<System.String, System.Double>();
			foreach (var 問題 in 問題字典.Values)
			{
				var 分子 = 0D;
				var 分母 = 問題.用戶標識字典.Count * (問題.用戶標識字典.Count - 1) / 2.0;
				foreach (var 用戶甲 in 問題.用戶標識字典.Keys)
				{
					foreach (var 用戶乙 in 問題.用戶標識字典.Keys)
					{
						if (!用戶共答數字典.ContainsKey(用戶甲 + "," + 用戶乙))
							continue;

						分子 += 用戶共答數字典[用戶甲 + "," + 用戶乙];
					}
				}

				問題權重字典[問題.標識] = 分子 / (1 + 分母);
			}
			#endregion
			
			#region 預測
			var 執行緒數組 = new System.Threading.Thread[6];
			var 等待控制代碼數組 = new System.Threading.ManualResetEvent[執行緒數組.Length];

			for (var 甲 = 0; 甲 < 執行緒數組.Length; 甲++)
			{
				等待控制代碼數組[甲] = new System.Threading.ManualResetEvent(false);
				執行緒數組[甲] = new System.Threading.Thread((參數) =>
				{
					var 子 = (System.Int32)參數;
					for (var 乙 = 0; 乙 < 預測結果清單.Count; 乙++)
					{
						if (乙 % 執行緒數組.Length != 子)
							continue;

						類別_邀請 邀請 = 預測結果清單[乙];

						var 問題用戶正搭配度 = 計算問題用戶正搭配度(邀請.問題標識, 邀請.用戶標識, 問題字典, 用戶字典, 問題權重字典);
						var 問題用戶逆搭配度 = 計算問題用戶逆搭配度(邀請.問題標識, 邀請.用戶標識, 問題字典, 用戶字典, 問題權重字典);
						var 問題用戶搭配度寅 = 計算問題用戶搭配度寅(邀請.問題標識, 邀請.用戶標識, 問題字典, 用戶字典, 問題權重字典);
						var 用戶回答率 = 用戶字典[邀請.用戶標識].訓練集回答數 / (用戶字典[邀請.用戶標識].訓練集被邀請數 + 1);

						邀請.值 = 用戶回答率 + 0.6 * 問題用戶正搭配度 + 0.6 * 問題用戶逆搭配度 + 0.4 * 問題用戶搭配度寅;

						if (既答雜湊集.Contains(邀請.問題標識 + "," + 邀請.用戶標識))
							邀請.值 = -1;
					}
					等待控制代碼數組[子].Set();
				});
				執行緒數組[甲].Start(甲);
			}
			System.Threading.WaitHandle.WaitAll(等待控制代碼數組);
			#endregion

			#region 整理
			預測結果清單.Sort
			(
				new System.Comparison<類別_邀請>
				(
					delegate (類別_邀請 甲, 類別_邀請 乙)
					{
						if (甲.值 > 乙.值)
							return 1;
						else if (甲.值 == 乙.值)
						{
							if (用戶字典[甲.用戶標識].問題標識字典.Count < 用戶字典[乙.用戶標識].問題標識字典.Count)
								return 1;
							else if (用戶字典[甲.用戶標識].問題標識字典.Count == 用戶字典[乙.用戶標識].問題標識字典.Count)
								return 0;
						}
						return -1;
					}
				)
			);

			for (var 甲 = 0; 甲 < 預測結果清單.Count; 甲++)
				預測結果清單[甲].值 = (甲 + 1) * 0.000001;
			#endregion

			return 預測結果清單;
		}

		static System.Double 計算問題用戶正搭配度(System.String 問題標識, System.String 用戶標識, System.Collections.Generic.Dictionary<System.String, 類別_問題> 問題字典, System.Collections.Generic.Dictionary<System.String, 類別_用戶> 用戶字典, System.Collections.Generic.Dictionary<System.String, System.Double> 問題權重字典)
		{
			var 搭配度分子 = 1D;
			var 搭配度分母 = 1D;

			foreach (var 問題標識鍵值對 in 用戶字典[用戶標識].問題標識字典)
			{
				var 交 = 0D;
				var 全 = 0D;

				foreach (var 用戶標識鍵值對 in 問題字典[問題標識].用戶標識字典)
				{
					if (用戶標識鍵值對.Value == 1)
					{
						全 += 3;
						if (問題字典[問題標識鍵值對.Key].用戶標識字典.ContainsKey(用戶標識鍵值對.Key))
							交 += 3;
					}
					else if (用戶標識鍵值對.Value == 0)
					{
						全++;
						if (問題字典[問題標識鍵值對.Key].用戶標識字典.ContainsKey(用戶標識鍵值對.Key))
							交++;
					}
					else if (用戶標識鍵值對.Value == -1)
					{
						全 += 0.5;
						if (問題字典[問題標識鍵值對.Key].用戶標識字典.ContainsKey(用戶標識鍵值對.Key))
							交 += 0.5;
					}
				}

				if (問題標識鍵值對.Value == 1)
					搭配度分子 += System.Math.Pow(交, 2) / (1 + 全);
				搭配度分母 += System.Math.Pow(交, 2) / (1 + 全);
			}

			return 搭配度分子 / 搭配度分母;
		}

		static System.Double 計算問題用戶逆搭配度(System.String 問題標識, System.String 用戶標識, System.Collections.Generic.Dictionary<System.String, 類別_問題> 問題字典, System.Collections.Generic.Dictionary<System.String, 類別_用戶> 用戶字典, System.Collections.Generic.Dictionary<System.String, System.Double> 問題權重字典)
		{
			var 搭配度分子 = 1D;
			var 搭配度分母 = 1D;

			foreach (var 用戶標籤鍵值對 in 問題字典[問題標識].用戶標識字典)
			{
				var 交 = 0D;
				var 全 = 0D;

				foreach (var 問題標識鍵值對 in 用戶字典[用戶標識].問題標識字典)
				{
					if (問題標識鍵值對.Value == 1)
					{
						全 += 1;
						if (用戶字典[用戶標籤鍵值對.Key].問題標識字典.ContainsKey(問題標識鍵值對.Key))
							交 += 1;
					}
					else if (問題標識鍵值對.Value == 0)
					{
						全 += 1;
						if (用戶字典[用戶標籤鍵值對.Key].問題標識字典.ContainsKey(問題標識鍵值對.Key))
							交 += 1;
					}
					else if (問題標識鍵值對.Value == -1)
					{
						全 += 1;
						if (用戶字典[用戶標籤鍵值對.Key].問題標識字典.ContainsKey(問題標識鍵值對.Key))
							交 += 1;
					}
				}

				if (用戶標籤鍵值對.Value == 1)
					搭配度分子 += System.Math.Pow(交, 3) / (全 + 1);
				if (用戶標籤鍵值對.Value >= 0)
					搭配度分母 += System.Math.Pow(交, 3) / (全 + 1);
			}

			return 搭配度分子 / 搭配度分母;
		}

		static System.Double 計算問題用戶搭配度寅(System.String 問題標識, System.String 用戶標識, System.Collections.Generic.Dictionary<System.String, 類別_問題> 問題字典, System.Collections.Generic.Dictionary<System.String, 類別_用戶> 用戶字典, System.Collections.Generic.Dictionary<System.String, System.Double> 問題權重字典)
		{
			var 搭配度分子 = 1D;
			var 搭配度分母 = 1D;

			foreach (var 問題標識鍵值對 in 用戶字典[用戶標識].問題標識字典)
			{
				var 交 = 0D;
				var 全 = 0D;

				foreach (var 用戶標識鍵值對 in 問題字典[問題標識].用戶標識字典)
				{
					if (用戶標識鍵值對.Value == 1)
					{
						全 += 3;
						if (問題字典[問題標識鍵值對.Key].用戶標識字典.ContainsKey(用戶標識鍵值對.Key))
							交 += 3;
					}
					else if (用戶標識鍵值對.Value == 0)
					{
						全++;
						if (問題字典[問題標識鍵值對.Key].用戶標識字典.ContainsKey(用戶標識鍵值對.Key))
							交++;
					}
					else if (用戶標識鍵值對.Value == -1)
					{
						全++;
						if (問題字典[問題標識鍵值對.Key].用戶標識字典.ContainsKey(用戶標識鍵值對.Key))
							交++;
					}
				}

				if (問題標識鍵值對.Value == 1)
					搭配度分子 += System.Math.Pow(交, 3) / (1 + 全);
				if (問題標識鍵值對.Value >= 0)
					搭配度分母 += System.Math.Pow(交, 3) / (1 + 全);
			}

			foreach (var 用戶標籤鍵值對 in 問題字典[問題標識].用戶標識字典)
			{
				var 交 = 0D;
				var 全 = 0D;

				foreach (var 問題標識鍵值對 in 用戶字典[用戶標識].問題標識字典)
				{
					if (問題標識鍵值對.Value == 1)
					{
						全 += 3;
						if (用戶字典[用戶標籤鍵值對.Key].問題標識字典.ContainsKey(問題標識鍵值對.Key))
							交 += 3;
					}
					else if (問題標識鍵值對.Value == 0)
					{
						全 += 1;
						if (用戶字典[用戶標籤鍵值對.Key].問題標識字典.ContainsKey(問題標識鍵值對.Key))
							交 += 1;
					}
					else if (問題標識鍵值對.Value == -1)
					{
						全 += 1;
						if (用戶字典[用戶標籤鍵值對.Key].問題標識字典.ContainsKey(問題標識鍵值對.Key))
							交 += 1;
					}
				}

				if (用戶標籤鍵值對.Value == 1)
					搭配度分子 += System.Math.Pow(交, 3) / (1 + 全);
				if (用戶標籤鍵值對.Value >= 0)
					搭配度分母 += System.Math.Pow(交, 3) / (1 + 全);
			}

			return 搭配度分子 / 搭配度分母;
		}
	}
}
