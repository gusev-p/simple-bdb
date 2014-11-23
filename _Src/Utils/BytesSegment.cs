using System;
using JetBrains.Annotations;

namespace SimpleBdb.Utils
{
	public struct BytesSegment
	{
		private readonly byte[] bytes;
		public int Offset { get; private set; }
		public int Length { get; private set; }

		public BytesSegment([NotNull] byte[] bytes)
			: this(bytes, 0, bytes.Length)
		{
		}

		public BytesSegment([NotNull] byte[] bytes, int offset)
			: this(bytes, offset, bytes.Length - offset)
		{
		}

		public BytesSegment([NotNull] byte[] bytes, int offset, int length) : this()
		{
			if (offset < 0 || length < 0 || offset + length > bytes.Length)
			{
				const string messageFormat = "invalid arguments, bytes length [{0}], offset [{1}], length [{2}]";
				throw new InvalidOperationException(string.Format(messageFormat, bytes.Length, offset, length));
			}
			this.bytes = bytes;
			Offset = offset;
			Length = length;
		}

		[NotNull]
		public byte[] DangerousGetBytes()
		{
			return bytes;
		}

		[NotNull]
		public byte[] ToByteArray()
		{
			return Offset == 0 && Length == bytes.Length ? bytes : CopyToByteArray();
		}

		[NotNull]
		public byte[] CopyToByteArray()
		{
			var result = new byte[Length];
			CopyTo(0, result, 0, Length);
			return result;
		}

		public void CopyTo(int sourceOffset, [NotNull] byte[] target, int targetOffset, int bytesToCopy)
		{
			if (sourceOffset < 0 || bytesToCopy < 0 || sourceOffset + bytesToCopy > Length)
				throw new InvalidOperationException(string.Format("invalid arguments, sourceOffset [{0}], bytesToCopy  [{1}], {2}",
					sourceOffset, bytesToCopy, this));
			Array.Copy(bytes, Offset  + sourceOffset, target, targetOffset, bytesToCopy);
		}

		public byte this[int i]
		{
			get
			{
				if (i < 0 || i + 1 > Length)
					throw new InvalidOperationException(string.Format("invalid index [{0}], {1}", i, this));
				return bytes[Offset + i];
			}
		}

		public override string ToString()
		{
			return string.Format("BytesSegment, bytes length [{0}], Offset [{1}], Length: [{2}]", bytes.Length, Offset, Length);
		}
	}
}