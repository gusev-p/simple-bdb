using System.Runtime.InteropServices;

namespace SimpleBdb.Utils
{
	[StructLayout(LayoutKind.Sequential, Pack = 1)]
	public struct SegmentPosition
	{
		public uint start;
		public uint length;
	}
}