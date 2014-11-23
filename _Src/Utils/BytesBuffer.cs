using JetBrains.Annotations;

namespace SimpleBdb.Utils
{
	public class BytesBuffer
	{
		[NotNull]
		public byte[] DangerousBytes { get; set; }

		public int Length { get; set; }
	}
}