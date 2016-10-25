namespace 純規則.桑楡.今日頭條競賽
{
	public static partial class 靜態類別_今日頭條競賽
	{
		static void Main(System.String[] 參)
		{
			var 問題字典 = new System.Collections.Generic.Dictionary<System.String, 類別_問題>();
			var 用戶字典 = new System.Collections.Generic.Dictionary<System.String, 類別_用戶>();
			var 訓練集清單 = new System.Collections.Generic.List<類別_邀請>();
			var 測試集清單 = new System.Collections.Generic.List<類別_邀請>();

			匯入(問題字典, 用戶字典, 訓練集清單, 測試集清單);

			var 結果甲 = 靜態類別_預測甲.預測(問題字典, 用戶字典, 訓練集清單, 測試集清單);
			匯出(結果甲, "預測.csv");
		}

		static void 匯入(System.Collections.Generic.Dictionary<System.String, 類別_問題> 問題字典, System.Collections.Generic.Dictionary<System.String, 類別_用戶> 用戶字典, System.Collections.Generic.List<類別_邀請> 訓練集清單, System.Collections.Generic.List<類別_邀請> 測試集清單)
		{
			#region 問題
			var 問題流閱讀器 = new System.IO.StreamReader(System.IO.File.OpenRead("question_info.txt"));
			for (;;)
			{
				var 字串 = 問題流閱讀器.ReadLine();
				if (字串 == null)
					break;			

				var 字串分詞 = 字串.Split('\t');
				問題字典[字串分詞[0]] = new 類別_問題(字串分詞[0], System.Int32.Parse(字串分詞[1]), 字串分詞[2], 字串分詞[3], System.Int32.Parse(字串分詞[4]), System.Int32.Parse(字串分詞[5]), System.Int32.Parse(字串分詞[6]));
			}
			問題流閱讀器.Close();
			#endregion

			#region 用戶
			var 用戶流閱讀器 = new System.IO.StreamReader(System.IO.File.OpenRead("user_info.txt"));
			for (;;)
			{
				var 字串 = 用戶流閱讀器.ReadLine();
				if (字串 == null)
					break;

				var 字串分詞 = 字串.Split('\t');
				用戶字典[字串分詞[0]] = new 類別_用戶(字串分詞[0], 字串分詞[1], 字串分詞[2], 字串分詞[3]);
			}
			用戶流閱讀器.Close();
			#endregion

			#region 訓練集
			var 訓練集流閱讀器 = new System.IO.StreamReader(System.IO.File.OpenRead("invited_info_train.txt"));
			for (;;)
			{
				var 字串 = 訓練集流閱讀器.ReadLine();
				if (字串 == null)
					break;

				var 字串分詞 = 字串.Split('\t');
				訓練集清單.Add(new 類別_邀請(字串分詞[0], 字串分詞[1], System.Double.Parse(字串分詞[2])));
				
				問題字典[字串分詞[0]].用戶標識字典[字串分詞[1]] = System.Double.Parse(字串分詞[2]);
				用戶字典[字串分詞[1]].問題標識字典[字串分詞[0]] = System.Double.Parse(字串分詞[2]);
			}
			訓練集流閱讀器.Close();
			#endregion

			#region 測試集
			var 測試集流閱讀器 = new System.IO.StreamReader(System.IO.File.OpenRead("validate_nolabel.txt"));
			for (;;)
			{
				var 字串 = 測試集流閱讀器.ReadLine();
				if (字串 == null)
					break;

				var 字串分詞 = 字串.Split(',');
				測試集清單.Add(new 類別_邀請(字串分詞[0], 字串分詞[1], -1));
				問題字典[字串分詞[0]].用戶標識字典[字串分詞[1]] = -1;
				用戶字典[字串分詞[1]].問題標識字典[字串分詞[0]] = -1;
			}
			測試集流閱讀器.Close();
			#endregion
		}

		static void 匯出(System.Collections.Generic.List<類別_邀請> 預測清單, System.String 檔案名)
		{
			var 流書寫器 = new System.IO.StreamWriter(檔案名);
			流書寫器.WriteLine("qid,uid,label");
			foreach (var 邀請 in 預測清單)
				流書寫器.WriteLine(邀請.問題標識 + "," + 邀請.用戶標識 + "," + 邀請.值.ToString("F5"));

			流書寫器.Close();
		}
	}
}