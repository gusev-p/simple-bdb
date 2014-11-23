using JetBrains.Annotations;

namespace SimpleBdb.Utils
{
	public class Boundary
	{
		[NotNull]
		public byte[] Value { get; private set; }

		public bool Inclusive { get; private set; }

		public Boundary([NotNull] byte[] value, bool inclusive)
		{
			Value = value;
			Inclusive = inclusive;
		}
	};
}