using System.Collections.Generic;
using JetBrains.Annotations;
using SimpleBdb.Driver;
using SimpleBdb.Utils;

namespace SimpleBdb.Extensions
{
	public static class DatabaseExtensions
	{
		[NotNull]
		public static ICursor QueryAll([NotNull] this Database database)
		{
			return database.Query(Range.Line(), Direction.Ascending);
		}

		[NotNull]
		public static ICursor Query([NotNull] this Database database, [NotNull] Range range, Direction direction, int skip = 0)
		{
			return database.Query(range, direction, skip, -1);
		}

		public static uint GetCount([NotNull] this Database database, [NotNull] Range range)
		{
			using (var cursor = database.Query(range, Direction.Ascending))
				return cursor.GetTotalCount();
		}

		[CanBeNull]
		public static BytesBuffer Find([NotNull] this Database database, [NotNull] byte[] key)
		{
			return database.Find(new BytesSegment(key));
		}

		public static void Set([NotNull] this Database database, BytesSegment key, BytesSegment value)
		{
			database.Remove(key);
			database.Add(key, value);
		}

		public static void Set([NotNull] this Database database, [NotNull] byte[] key, [NotNull] byte[] value)
		{
			database.Set(new BytesSegment(key), new BytesSegment(value));
		}

		public static void Add(this Database database, [NotNull] byte[] key, [NotNull] byte[] value)
		{
			database.Add(new BytesSegment(key), new BytesSegment(value));
		}

		public static void AddBatch([NotNull] this Database database, [NotNull] List<BytesSegment> keys, BytesSegment value)
		{
			foreach (var key in keys)
				database.Add(key, value);
		}

		public static void RemoveBatch([NotNull] this Database database, [NotNull] List<BytesSegment> keys)
		{
			foreach (var key in keys)
				database.Remove(key);
		}
	}
}