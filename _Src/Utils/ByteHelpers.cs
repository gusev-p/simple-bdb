using System.Runtime.InteropServices;

namespace SimpleBdb.Utils
{
	public static class ByteHelpers
	{
		public static bool Equals(byte[] x, byte[] y)
		{
			if (x == null ^ y == null) return false;
			if (ReferenceEquals(x, y)) return true;
			if (x.Length != y.Length) return false;
			return memcmp(x, y, x.Length) == 0;
		}

		public static int Compare(byte[] x, byte[] y)
		{
			if (x == null) return y == null ? 0 : -1;
			if (y == null) return 1;
			if (ReferenceEquals(x, y)) return 0;
			if (x.Length < y.Length)
			{
				var res = memcmp(x, y, x.Length);
				return res != 0 ? res : -1;
			}
			if (x.Length > y.Length)
			{
				var res = memcmp(x, y, y.Length);
				return res != 0 ? res : 1;
			}
			return memcmp(x, y, x.Length);
		}

		[DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
		private static extern int memcmp(byte[] b1, byte[] b2, long count);
	}
}