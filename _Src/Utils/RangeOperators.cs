using System;
using JetBrains.Annotations;

namespace SimpleBdb.Utils
{
	public static class RangeOperators
	{
		[NotNull]
		public static Range Prepend([NotNull] this Range range, [NotNull] byte[] prefix)
		{
			return new Range(range.Left.Prepend(prefix), range.Right.Prepend(prefix));
		}

		[NotNull]
		public static Range IntersectWith([NotNull] this Range a, [NotNull] Range b)
		{
			return new Range(MaxLeft(a.Left, b.Left), MinRight(a.Right, b.Right));
		}

		public static bool IsEmpty([NotNull] this Range range)
		{
			if (range.Left == null || range.Right == null)
				return false;
			var compareResult = ByteHelpers.Compare(range.Left.Value, range.Right.Value);
			if (compareResult != 0)
				return compareResult > 0;
			return !range.Left.Inclusive || !range.Right.Inclusive;
		}

		[CanBeNull]
		private static Boundary MaxLeft([CanBeNull] Boundary a, [CanBeNull] Boundary b)
		{
			return CompareLeft(a, b) > 0 ? a : b;
		}

		[CanBeNull]
		private static Boundary MinRight([CanBeNull] Boundary a, [CanBeNull] Boundary b)
		{
			return CompareRight(a, b) < 0 ? a : b;
		}

		private static int CompareLeft([CanBeNull] Boundary a, [CanBeNull] Boundary b)
		{
			if (a == null && b == null)
				return 0;
			if (a == null)
				return -1;
			if (b == null)
				return 1;
			var result = ByteHelpers.Compare(a.Value, b.Value);
			if (result != 0)
				return result;
			if (a.Inclusive == b.Inclusive)
				return 0;
			if (a.Inclusive)
				return -1;
			return 1;
		}

		private static int CompareRight([CanBeNull] Boundary a, [CanBeNull] Boundary b)
		{
			if (a == null && b == null)
				return 0;
			if (a == null)
				return 1;
			if (b == null)
				return -1;
			var result = ByteHelpers.Compare(a.Value, b.Value);
			if (result != 0)
				return result;
			if (a.Inclusive == b.Inclusive)
				return 0;
			if (a.Inclusive)
				return 1;
			return -1;
		}

		[CanBeNull]
		private static Boundary Prepend([CanBeNull] this Boundary boundary, [NotNull] byte[] prefix)
		{
			if (boundary == null)
				return null;
			var newValue = new byte[prefix.Length + boundary.Value.Length];
			Array.Copy(prefix, newValue, prefix.Length);
			Array.Copy(boundary.Value, 0, newValue, prefix.Length, boundary.Value.Length);
			return new Boundary(newValue, boundary.Inclusive);
		}
	}
}