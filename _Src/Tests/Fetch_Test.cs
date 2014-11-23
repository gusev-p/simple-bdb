using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using SimpleBdb.Driver;
using SimpleBdb.Extensions;
using SimpleBdb.Tests.Helpers;
using SimpleBdb.Utils;
using Environment = SimpleBdb.Driver.Environment;
using TestBase = SimpleBdb.Tests.Helpers.TestBase;

namespace SimpleBdb.Tests
{
	public class FetchTest : TestBase
	{
		[Test]
		public void FetchEmptyKeys()
		{
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				var bytesTable = DoFetch(db, FetchOptions.Keys, 10);
				Assert.That(bytesTable, Is.Not.Null);
				Assert.That(bytesTable.RowsCount, Is.EqualTo(0));
				Assert.That(bytesTable.store, Is.Not.Null);
				Assert.That(bytesTable.positions, Is.Not.Null);
				Assert.That(bytesTable.ColumnsCount, Is.EqualTo(1));
				Assert.That(bytesTable.store.Length, Is.EqualTo(10*100));
				Assert.That(bytesTable.positions.Length, Is.EqualTo(10));

				var error = Assert.Throws<InvalidOperationException>(() => bytesTable.GetSegment(0, 0));
				Assert.That(error.Message, Is.EqualTo("invalid arguments, row [0], column [0], RowsCount [0], ColumnsCount [1]"));
			}
		}

		private static BytesTable DoFetch(Database database, FetchOptions options, int take = -1)
		{
			using (var cursor = database.Query(Range.Line(), Direction.Ascending, 0, take))
				return cursor.Fetch(options);
		}

		[Test]
		public void FetchSingleKey()
		{
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(1.AsKey(), 2.AsKey());
				var fetchResult = DoFetch(db, FetchOptions.Keys, 1);
				Assert.That(fetchResult.store.Length, Is.EqualTo(defaultDbConfig.KeyBufferConfig.Size));
				Assert.That(fetchResult.store[0], Is.EqualTo((byte)1));
				Assert.That(fetchResult.RowsCount, Is.EqualTo(1));
				Assert.That(fetchResult.positions[0].start, Is.EqualTo(0));
				Assert.That(fetchResult.positions[0].length, Is.EqualTo(1));
			}
		}

		[Test]
		public void FetchSingleKeyAndValue()
		{
			defaultDbConfig.KeyBufferConfig = BytesBufferConfig.FixedTo(4);
			defaultDbConfig.ValueBufferConfig = BytesBufferConfig.FixedTo(5);
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(new byte[] { 1, 2, 3, 4 }, new byte[] { 101, 102, 103, 104, 105 });
				var fetchResult = DoFetch(db, FetchOptions.KeysAndValues, 1);
				Assert.That(fetchResult.store, Is.EqualTo(new byte[] { 1, 2, 3, 4, 101, 102, 103, 104, 105 }));
				Assert.That(fetchResult.RowsCount, Is.EqualTo(1));
				Assert.That(fetchResult.positions.Length, Is.EqualTo(2));
				Assert.That(fetchResult.positions[0].start, Is.EqualTo(0));
				Assert.That(fetchResult.positions[0].length, Is.EqualTo(4));
				Assert.That(fetchResult.positions[1].start, Is.EqualTo(4));
				Assert.That(fetchResult.positions[1].length, Is.EqualTo(5));
			}
		}

		[Test]
		public void CanFetchMultipleKeysAndValues()
		{
			defaultDbConfig.KeyBufferConfig = BytesBufferConfig.FixedTo(4);
			defaultDbConfig.ValueBufferConfig = BytesBufferConfig.FixedTo(4);
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				var bytes = new byte[] { 0, 1, 2, 3 };
				var expectedStore = new List<byte>();
				var expectedPositions = new SegmentPosition[100];
				var storePosition = 0u;
				for (var i = 0; i < 50; i++)
				{
					var keys = bytes.Select(x => (byte)(i * 4 + x)).ToArray();
					var values = bytes.Select(x => (byte)(i * 4 + x + 10)).ToArray();
					db.Add(keys, values);
					expectedStore.AddRange(keys);
					expectedStore.AddRange(values);
					expectedPositions[i * 2].start = storePosition;
					expectedPositions[i * 2].length = 4;
					storePosition += 4;
					expectedPositions[i * 2 + 1].start = storePosition;
					expectedPositions[i * 2 + 1].length = 4;
					storePosition += 4;
				}
				var fetchResult = DoFetch(db, FetchOptions.KeysAndValues, 50);
				Assert.That(fetchResult.store, Is.EqualTo(expectedStore.ToArray()));
				Assert.That(fetchResult.RowsCount, Is.EqualTo(50));
				Assert.That(fetchResult.positions, Is.EqualTo(expectedPositions));
			}
		}

		[Test]
		public void NoExplicitTake_FetchTotalRecordsCount()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(1.AsKey(), 2.AsKey());
				db.Add(new byte[] { 7, 2, 1 }, new byte[] { 99, 7, 1, 67 });
				var fetchResult = DoFetch(db, FetchOptions.KeysAndValues);
				Assert.That(fetchResult.RowsCount, Is.EqualTo(2));

				var expectedStore = new byte[2 * (defaultDbConfig.KeyBufferConfig.Size + defaultDbConfig.ValueBufferConfig.Size)];
				expectedStore[0] = 1;
				expectedStore[100] = 2;
				expectedStore[200] = 7;
				expectedStore[201] = 2;
				expectedStore[202] = 1;
				expectedStore[300] = 99;
				expectedStore[301] = 7;
				expectedStore[302] = 1;
				expectedStore[303] = 67;
				Assert.That(fetchResult.store, Is.EqualTo(expectedStore));

				Assert.That(fetchResult.positions.Length, Is.EqualTo(4));
				Assert.That(fetchResult.positions[0].start, Is.EqualTo(0));
				Assert.That(fetchResult.positions[0].length, Is.EqualTo(1));
				Assert.That(fetchResult.positions[1].start, Is.EqualTo(100));
				Assert.That(fetchResult.positions[1].length, Is.EqualTo(1));

				Assert.That(fetchResult.positions[2].start, Is.EqualTo(200));
				Assert.That(fetchResult.positions[2].length, Is.EqualTo(3));
				Assert.That(fetchResult.positions[3].start, Is.EqualTo(300));
				Assert.That(fetchResult.positions[3].length, Is.EqualTo(4));
			}
		}
	
		[Test]
		public void MaxValueExplicitTake_FetchTotalRecordsCount()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(1.AsKey(), 2.AsKey());
				db.Add(new byte[] { 7, 2, 1 }, new byte[] { 99, 7, 1, 67 });
				var fetchResult = DoFetch(db, FetchOptions.KeysAndValues, int.MaxValue);
				Assert.That(fetchResult.RowsCount, Is.EqualTo(2));
			}
		}

		[Test]
		public void SomeRecordsHaveBeenRead_DoNotFetchItAgain()
		{
			defaultDbConfig.EnableRecno = true;
			defaultDbConfig.KeyBufferConfig = BytesBufferConfig.FixedTo(4);
			defaultDbConfig.ValueBufferConfig = BytesBufferConfig.FixedTo(4);
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(1.AsKey(), 2.AsKey());
				db.Add(3.AsKey(), 4.AsKey());
				using (var cursor = db.Query(Range.Line(), Direction.Ascending, 0, -1))
				{
					BytesRecord _;
					Assert.That(cursor.Read(out _));

					var fetchResult = cursor.Fetch(FetchOptions.KeysAndValues);
					Assert.That(fetchResult.RowsCount, Is.EqualTo(1));
					Assert.That(fetchResult.store, Is.EqualTo(new byte[] { 3, 0, 0, 0, 4, 0, 0, 0 }));
					Assert.That(fetchResult.positions.Length, Is.EqualTo(2));
					Assert.That(fetchResult.positions[0].start, Is.EqualTo(0));
					Assert.That(fetchResult.positions[0].length, Is.EqualTo(1));
					Assert.That(fetchResult.positions[1].start, Is.EqualTo(4));
					Assert.That(fetchResult.positions[1].length, Is.EqualTo(1));
				}
			}
		}

		[Test]
		public void DoNotAllocateSpaceForSkippedRecords()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(1.AsKey(), 2.AsKey());
				db.Add(3.AsKey(), 4.AsKey());
				db.Add(5.AsKey(), 6.AsKey());
				using (var cursor = db.Query(Range.Line(), Direction.Ascending, 1, -1))
				{
					var fetchResult = cursor.Fetch(FetchOptions.Keys);
					Assert.That(fetchResult.store.Length, Is.EqualTo(defaultDbConfig.KeyBufferConfig.Size * 2));
					Assert.That(fetchResult.store[0], Is.EqualTo(3));
					Assert.That(fetchResult.store[100], Is.EqualTo(5));
					Assert.That(fetchResult.RowsCount, Is.EqualTo(2));
					Assert.That(fetchResult.positions[0].start, Is.EqualTo(0));
					Assert.That(fetchResult.positions[0].length, Is.EqualTo(1));
					Assert.That(fetchResult.positions[1].start, Is.EqualTo(100));
					Assert.That(fetchResult.positions[1].length, Is.EqualTo(1));
				}
			}
		}

		[Test]
		public void RetryFetchWhenBufferSmall()
		{
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(1.AsKey(), 2.AsKey());
				db.Add(new byte[] { 1, 2, 3, 4, 5 }, 4.AsKey());
			}

			defaultDbConfig.KeyBufferConfig = BytesBufferConfig.GrowFrom(4);
			defaultDbConfig.ValueBufferConfig = BytesBufferConfig.GrowFrom(4);
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			using (var cursor = db.Query(Range.Line(), Direction.Ascending, 0, 2))
			{
				var fetchResult = cursor.Fetch(FetchOptions.Keys);
				Assert.That(fetchResult.store.Length, Is.EqualTo(5 * 2));
				Assert.That(fetchResult.store, Is.EqualTo(new byte[] { 1, 0, 0, 0, 1, 2, 3, 4, 5, 0 }));
				Assert.That(fetchResult.RowsCount, Is.EqualTo(2));
				Assert.That(fetchResult.positions[0].start, Is.EqualTo(0));
				Assert.That(fetchResult.positions[0].length, Is.EqualTo(1));
				Assert.That(fetchResult.positions[1].start, Is.EqualTo(4));
				Assert.That(fetchResult.positions[1].length, Is.EqualTo(5));
			}
		}

		[Test]
		public void MultipleRetries()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(1.AsKey(), 200.AsKey());
				db.Add(2.AsKey(), new byte[] { 1, 2, 3, 4, 5 });
				db.Add(3.AsKey(), new byte[] { 10, 20, 30, 40, 50, 60 });
				db.Add(4.AsKey(), new byte[] { 70, 80, 90, 100, 101, 102, 103 });
			}

			defaultDbConfig.KeyBufferConfig = BytesBufferConfig.FixedTo(4);
			defaultDbConfig.ValueBufferConfig = BytesBufferConfig.GrowFrom(4);
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			using (var cursor = db.Query(Range.Line(), Direction.Ascending, 0, 4))
			{
				var fetchResult = cursor.Fetch(FetchOptions.Values);
				Assert.That(fetchResult.store.Length, Is.EqualTo(7 * 4));
				Assert.That(fetchResult.store, Is.EqualTo(new byte[]
				{
					200, 0, 0, 0,
					1, 2, 3, 4, 5,
					10, 20, 30, 40, 50, 60,
					70, 80, 90, 100, 101, 102, 103,
					0, 0, 0,
					0, 0,
					0
				}));
				Assert.That(fetchResult.RowsCount, Is.EqualTo(4));
				Assert.That(fetchResult.positions[0].start, Is.EqualTo(0));
				Assert.That(fetchResult.positions[0].length, Is.EqualTo(1));
				Assert.That(fetchResult.positions[1].start, Is.EqualTo(4));
				Assert.That(fetchResult.positions[1].length, Is.EqualTo(5));
				Assert.That(fetchResult.positions[2].start, Is.EqualTo(9));
				Assert.That(fetchResult.positions[2].length, Is.EqualTo(6));
				Assert.That(fetchResult.positions[3].start, Is.EqualTo(15));
				Assert.That(fetchResult.positions[3].length, Is.EqualTo(7));
			}
		}
	}
}