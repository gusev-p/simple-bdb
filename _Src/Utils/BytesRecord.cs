using JetBrains.Annotations;

namespace SimpleBdb.Utils
{
	public class BytesRecord
	{
		[NotNull]
		public BytesBuffer Key { get; private set; }

		[NotNull]
		public BytesBuffer Value { get; private set; }

		public BytesRecord([NotNull] BytesBuffer key, [NotNull] BytesBuffer value)
		{
			Key = key;
			Value = value;
		}
	}
}