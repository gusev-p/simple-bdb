using NUnit.Framework;
using SimpleBdb.Driver;
using SimpleBdb.Extensions;
using SimpleBdb.Tests.Helpers;
using SimpleBdb.Utils;

namespace SimpleBdb.Tests
{
	[TestFixture]
	public class ApiFetchMultipleRangesTest : TestBase
	{
		[Test]
		public void Simple()
		{
			defaultDbConfig.ValueBufferConfig = BytesBufferConfig.FixedTo(4);
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(new byte[] { 1, 2 }, new byte[] { 1 });
				db.Add(new byte[] { 2, 3 }, new byte[] { 2 });
				db.Add(new byte[] { 2, 1 }, new byte[] { 3 });
				var ranges = new[]
				{
					Range.Segment(new byte[] { 1, 2 }, new byte[] { 1, 2 }),
					Range.Segment(new byte[] { 2, 1 }, new byte[] { 2, 3 })
				};
				var result = db.Fetch(ranges, Direction.Ascending, 3, 1, FetchOptions.Values);
				Assert.That(result.store, Is.EqualTo(new byte[] { 3, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0 }));
				Assert.That(result.positions[0].start, Is.EqualTo(0));
				Assert.That(result.positions[0].length, Is.EqualTo(1));
				Assert.That(result.positions[1].start, Is.EqualTo(4));
				Assert.That(result.positions[1].length, Is.EqualTo(1));
				Assert.That(result.positions[2].start, Is.EqualTo(8));
				Assert.That(result.positions[2].length, Is.EqualTo(1));
			}
		}

		[Test]
		public void SingleRange()
		{
			defaultDbConfig.ValueBufferConfig = BytesBufferConfig.FixedTo(4);
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(new byte[] { 1, 2, 7 }, new byte[] { 100 });
				db.Add(new byte[] { 2, 3, 0 }, new byte[] { 101 });
				db.Add(new byte[] { 2, 1, 11 }, new byte[] { 102 });
				var ranges = new[] { Range.Line() };
				var result = db.Fetch(ranges, Direction.Ascending, 3, 2, FetchOptions.Values);
				Assert.That(result.store, Is.EqualTo(new byte[] { 100, 0, 0, 0, 102, 0, 0, 0, 101, 0, 0, 0 }));
			}
		}

		[Test]
		public void BufferCanGrow()
		{
			defaultDbConfig.ValueBufferConfig = BytesBufferConfig.GrowFrom(4);
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(new byte[] { 1, 2 }, new byte[] { 10 });
				db.Add(new byte[] { 2, 1 }, new byte[] { 20 });
				db.Add(new byte[] { 2, 3 }, new byte[] { 30, 40, 50, 60, 70 });
				var ranges = new[]
				{
					Range.Segment(new byte[] { 1, 2 }, new byte[] { 1, 2 }),
					Range.Segment(new byte[] { 2, 1 }, new byte[] { 2, 3 })
				};
				var result = db.Fetch(ranges, Direction.Ascending, 5, 1, FetchOptions.Values);
				Assert.That(result.RowsCount, Is.EqualTo(3));
				Assert.That(result.store, Is.EqualTo(new byte[] { 20, 0, 0, 0, 0, 10, 0, 0, 0, 0, 30, 40, 50, 60, 70, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }));
			}
		}

		[Test]
		public void FirstMergedReaderIterationInterruptedForGrow_ContinueFromLastNotRead()
		{
			defaultDbConfig.ValueBufferConfig = BytesBufferConfig.GrowFrom(4);
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(new byte[] { 1, 2 }, new byte[] { 1 });
				db.Add(new byte[] { 1, 3 }, new byte[] { 2 });
				db.Add(new byte[] { 2, 4 }, new byte[] { 3, 4, 5, 6, 7 });
				var ranges = new[]
				{
					Range.Segment(new byte[] { 1, 2 }, new byte[] { 1, 3 }),
					Range.Segment(new byte[] { 2, 4 }, new byte[] { 2, 4 })
				};
				var result = db.Fetch(ranges, Direction.Ascending, 5, 1, FetchOptions.Values);
				Assert.That(result.RowsCount, Is.EqualTo(3));
			}
		}

		[Test]
		public void BufferSmall_PreserveAlreadyFetchedContent()
		{
			defaultDbConfig.ValueBufferConfig = BytesBufferConfig.GrowFrom(4);
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(new byte[] { 1, 2 }, new byte[] { 10 });
				db.Add(new byte[] { 1, 1 }, new byte[] { 20 });
				db.Add(new byte[] { 2, 3 }, new byte[] { 30 });
				db.Add(new byte[] { 2, 4 }, new byte[] { 40, 60, 70, 80, 80 });
				var ranges = new[]
				{
					Range.Segment(new byte[] { 1, 1 }, new byte[] { 1, 2 }),
					Range.Segment(new byte[] { 2, 1 }, new byte[] { 2, 4 })
				};
				var result = db.Fetch(ranges, Direction.Ascending, 4, 1, FetchOptions.Values);
				Assert.That(result.RowsCount, Is.EqualTo(4));
				Assert.That(result.store, Is.EqualTo(new byte[]
				{
					20, 0, 0, 0,
					10, 0, 0, 0,
					30, 0, 0, 0, 0,
					40, 60, 70, 80, 80,
					0,
					0
				}));
			}
		}

		[Test]
		public void Interleaving()
		{
			defaultDbConfig.KeyBufferConfig = BytesBufferConfig.FixedTo(4);
			defaultDbConfig.ValueBufferConfig = BytesBufferConfig.FixedTo(4);
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(new byte[] { 1, 7 }, new byte[] { 100 });
				db.Add(new byte[] { 1, 4 }, new byte[] { 101 });
				db.Add(new byte[] { 2, 10 }, new byte[] { 102 });
				db.Add(new byte[] { 2, 6 }, new byte[] { 103 });
				db.Add(new byte[] { 2, 1 }, new byte[] { 104 });

				var ranges = new[]
				{
					Range.Segment(new byte[] { 1, 1 }, new byte[] { 1, 100 }),
					Range.Segment(new byte[] { 2, 1 }, new byte[] { 2, 200 })
				};
				var result = db.Fetch(ranges, Direction.Ascending, 5, 1, FetchOptions.KeysAndValues);
				Assert.That(result.RowsCount, Is.EqualTo(5));
				Assert.That(result.store, Is.EqualTo(new byte[]
				{
					2, 1, 0, 0, 104, 0, 0, 0,
					1, 4, 0, 0, 101, 0, 0, 0,
					2, 6, 0, 0, 103, 0, 0, 0,
					1, 7, 0, 0, 100, 0, 0, 0,
					2, 10, 0, 0, 102, 0, 0, 0
				}));
			}
		}

		[Test]
		public void EqualKeys()
		{
			defaultDbConfig.KeyBufferConfig = BytesBufferConfig.FixedTo(4);
			defaultDbConfig.ValueBufferConfig = BytesBufferConfig.FixedTo(4);
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(new byte[] { 1, 7 }, new byte[] { 100 });
				db.Add(new byte[] { 1, 4 }, new byte[] { 101 });
				db.Add(new byte[] { 2, 4 }, new byte[] { 102 });

				var ranges = new[]
				{
					Range.Segment(new byte[] { 1, 1 }, new byte[] { 1, 7 }),
					Range.Segment(new byte[] { 2, 1 }, new byte[] { 2, 200 })
				};
				var result = db.Fetch(ranges, Direction.Ascending, 3, 1, FetchOptions.Values);
				Assert.That(result.RowsCount, Is.EqualTo(3));
				Assert.That(result.store, Is.EqualTo(new byte[]
				{
					101, 0, 0, 0,
					102, 0, 0, 0,
					100, 0, 0, 0
				}));
			}
		}

		[Test]
		public void NoExplicitTake_AllRecordsReturned()
		{
			defaultDbConfig.ValueBufferConfig = BytesBufferConfig.FixedTo(4);
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(new byte[] { 1, 2 }, new byte[] { 1 });
				db.Add(new byte[] { 2, 3 }, new byte[] { 2 });
				db.Add(new byte[] { 2, 1 }, new byte[] { 3 });
				var ranges = new[]
				{
					Range.Segment(new byte[] { 1, 2 }, new byte[] { 1, 2 }),
					Range.Segment(new byte[] { 2, 1 }, new byte[] { 2, 3 })
				};
				Assert.That(db.Fetch(ranges, Direction.Ascending, -1, 1, FetchOptions.Values).RowsCount, Is.EqualTo(3));
				Assert.That(db.Fetch(ranges, Direction.Ascending, int.MaxValue, 1, FetchOptions.Values).RowsCount, Is.EqualTo(3));
			}
		}

		[Test]
		public void BufferSmallWhanCalculatingTotalCount_Retry()
		{
			defaultDbConfig.ValueBufferConfig = BytesBufferConfig.GrowFrom(4);
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(new byte[] { 1, 2 }, new byte[] { 1 });
				db.Add(new byte[] { 2, 3 }, new byte[] { 1, 2, 3, 4, 5 });
				var ranges = new[]
				{
					Range.Segment(new byte[] { 1, 2 }, new byte[] { 1, 2 }),
					Range.Segment(new byte[] { 2, 1 }, new byte[] { 2, 3 })
				};
				Assert.That(db.Fetch(ranges, Direction.Ascending, -1, 1, FetchOptions.Values).RowsCount, Is.EqualTo(2));
			}
		}

		[Test]
		public void DescendingDirection()
		{
			defaultDbConfig.ValueBufferConfig = BytesBufferConfig.FixedTo(4);
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(new byte[] { 1, 2 }, new byte[] { 1 });
				db.Add(new byte[] { 2, 3 }, new byte[] { 2 });
				db.Add(new byte[] { 2, 1 }, new byte[] { 3 });
				var ranges = new[]
				{
					Range.Segment(new byte[] { 1, 2 }, new byte[] { 1, 2 }),
					Range.Segment(new byte[] { 2, 1 }, new byte[] { 2, 3 })
				};
				var result = db.Fetch(ranges, Direction.Descending, 3, 1, FetchOptions.Values);
				Assert.That(result.store, Is.EqualTo(new byte[] { 2, 0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0 }));
				Assert.That(result.positions[0].start, Is.EqualTo(0));
				Assert.That(result.positions[0].length, Is.EqualTo(1));
				Assert.That(result.positions[1].start, Is.EqualTo(4));
				Assert.That(result.positions[1].length, Is.EqualTo(1));
				Assert.That(result.positions[2].start, Is.EqualTo(8));
				Assert.That(result.positions[2].length, Is.EqualTo(1));
			}
		}
	}
}