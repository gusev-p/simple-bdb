using System;
using JetBrains.Annotations;

namespace SimpleBdb.Utils
{
	public class Range
	{
		[CanBeNull]
		public Boundary Left { get; private set; }

		[CanBeNull]
		public Boundary Right { get; private set; }

		public Range([CanBeNull] Boundary left, [CanBeNull] Boundary right)
		{
			Left = left;
			Right = right;
		}

		[NotNull]
		public static Range Segment([NotNull] byte[] left, [NotNull] byte[] right)
		{
			return new Range(new Boundary(left, true), new Boundary(right, true));
		}

		[NotNull]
		public static Range PositiveRay([NotNull] byte[] left)
		{
			return new Range(new Boundary(left, true), null);
		}

		[NotNull]
		public static Range NegativeRay([NotNull] byte[] right)
		{
			return new Range(null, new Boundary(right, true));
		}

		[NotNull]
		public static Range NegativeOpenRay([NotNull] byte[] right)
		{
			return new Range(null, new Boundary(right, false));
		}

		private static readonly Range line = new Range(null, null);
		private static readonly Boundary exclusiveOne = new Boundary(new byte[] { 1 }, false);
		private static readonly Range emptyRange = new Range(exclusiveOne, exclusiveOne);

		[NotNull]
		public static Range Line()
		{
			return line;
		}

		[NotNull]
		public static Range Empty()
		{
			return emptyRange;
		}

		[NotNull]
		public static Range PositiveOpenRay([NotNull] byte[] left)
		{
			return new Range(new Boundary(left, false), null);
		}

		[NotNull]
		public static Range LeftOpenSegment([NotNull] byte[] left, [NotNull] byte[] right)
		{
			return new Range(new Boundary(left, false), new Boundary(right, true));
		}

		[NotNull]
		public static Range RightOpenSegment([NotNull] byte[] left, [NotNull] byte[] right)
		{
			return new Range(new Boundary(left, true), new Boundary(right, false));
		}

		[NotNull]
		public static Range Interval([NotNull] byte[] left, [NotNull] byte[] right)
		{
			return new Range(new Boundary(left, false), new Boundary(right, false));
		}

		[NotNull]
		public static Range Prefix([NotNull] byte[] prefix)
		{
			return Prefix(prefix, prefix);
		}

		[NotNull]
		public static Range Prefix([NotNull] byte[] left, [NotNull] byte[] right)
		{
			var rightBoundary = IncrementBytes(right);
			return new Range(new Boundary(left, true), rightBoundary == null ? null : new Boundary(rightBoundary, false));
		}

		[CanBeNull]
		public static byte[] IncrementBytes([NotNull] byte[] source)
		{
			var result = new byte[source.Length];
			Array.Copy(source, result, source.Length);
			for (var i = result.Length - 1; i >= 0; i--)
				if (result[i] == Byte.MaxValue)
					result[i] = 0;
				else
				{
					result[i]++;
					return result;
				}
			return null;
		}
	};
}