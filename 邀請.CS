namespace 純規則.桑楡.今日頭條競賽
{
	public class 類別_邀請
	{
		public System.String 問題標識 { get; }
		public System.String 用戶標識 { get; }
		public System.Double 值 { get; set; }

		public 類別_邀請(System.String 問題標識, System.String 用戶標識, System.Double 值)
		{
			this.問題標識 = 問題標識;
			this.用戶標識 = 用戶標識;
			this.值 = 值;
		}
	}
}
