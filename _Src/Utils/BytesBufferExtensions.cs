using System;
using JetBrains.Annotations;

namespace SimpleBdb.Utils
{
	public static class BytesBufferExtensions
	{
		[CanBeNull]
		public static byte[] AsByteArray([CanBeNull] this BytesBuffer bytesBuffer, bool allowCopy = false)
		{
			return bytesBuffer == null ? null : bytesBuffer.GetByteArray(allowCopy);
		}

		[NotNull]
		public static byte[] GetByteArray([NotNull] this BytesBuffer bytesBuffer, bool allowCopy = false)
		{
			if (bytesBuffer.Length == bytesBuffer.DangerousBytes.Length)
				return bytesBuffer.DangerousBytes;
			if (!allowCopy)
				throw new InvalidOperationException("copying is prohibited");
			return bytesBuffer.CopyToByteArray();
		}

		[NotNull]
		public static byte[] CopyToByteArray([NotNull] this BytesBuffer bytesBuffer)
		{
			return bytesBuffer.CopyToByteArray(0, bytesBuffer.Length);
		}

		[NotNull]
		public static byte[] CopyToByteArray([NotNull] this BytesBuffer bytesBuffer, int offset, int length)
		{
			if (offset < 0 || length < 0 || offset + length > bytesBuffer.Length)
				throw new InvalidOperationException(string.Format("Invalid args: bytesBuffer.Length={0}, offset={1}, length={2}", bytesBuffer.Length, offset, length));
			var result = new byte[length];
			Array.Copy(bytesBuffer.DangerousBytes, offset, result, 0, length);
			return result;
		}

		public static BytesSegment GetByteRange([NotNull] this BytesBuffer bytesBuffer, bool allowCopy = false)
		{
			return new BytesSegment(GetByteArray(bytesBuffer, allowCopy));
		}

		public static BytesSegment CopyToByteRange([NotNull] this BytesBuffer bytesBuffer)
		{
			return bytesBuffer.CopyToByteRange(0, bytesBuffer.Length);
		}

		public static BytesSegment CopyToByteRange([NotNull] this BytesBuffer bytesBuffer, int offset, int length)
		{
			return new BytesSegment(bytesBuffer.CopyToByteArray(offset, length));
		}
	}
}