using System;
using System.Linq;
using Moq;
using NUnit.Framework;
using SimpleBdb.Driver;
using SimpleBdb.Extensions;
using SimpleBdb.Tests.Helpers;
using SimpleBdb.Utils;
using Environment = SimpleBdb.Driver.Environment;
using Range = SimpleBdb.Utils.Range;

namespace SimpleBdb.Tests
{
	public class ApiQueryTest : TestBase
	{
		[Test]
		public void Simple()
		{
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add("k1", "v1");
				using (var reader = db.QueryByPrefix(new byte[0]))
					reader.AssertRead("k1", "v1").AssertStop();
			}
		}

		[Test]
		public void CanPositionToKeyEqualToKeyPrefix()
		{
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add("1", "v1").Add("2", "v2");
				using (var reader = db.QueryByPrefix(Bytes("2")))
					reader.AssertRead("2", "v2").AssertStop();
				using (var reader = db.QueryByPrefix(Bytes("1")))
					reader.AssertRead("1", "v1").AssertStop();
			}
		}

		[Test]
		public void CanPositionToKeyGreaterThanKeyPrefix()
		{
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add("34", "v34").Add("2", "v2").Add("1", "v1").Add("35", "v35");
				using (var reader = db.QueryByPrefix(Bytes("3")))
					reader.AssertRead("34").AssertRead("35").AssertStop();
			}
		}

		[Test]
		public void CanPositionToIndex()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add("1", "v1").Add("2", "v2").Add("23", "v23");
				using (var reader = db.QueryByPrefix(Bytes("2"), 1))
					reader.AssertRead("23", "v23").AssertStop();
			}
		}

		[Test]
		public void KeyBufferIsFixed_TryToAddLargerBytes_CorrectException()
		{
			defaultDbConfig.KeyBufferConfig = new BytesBufferConfig(4, true);
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				var localDb = db;
				var error = Assert.Throws<BdbException>(() => localDb.Add(new byte[] { 1, 2, 33, 12, 16, 7 }, new byte[] { 1 }));
				Assert.That(error.Message, Is.EqualTo(string.Format("requested length [6] is greater than size [4] of fixed buffer, keys, database (file name [{0}], database name [testDb])", fileFullPath)));
			}
		}

		[Test]
		public void KeyLengthCanBeSmallerThanFixedBufferSize()
		{
			defaultDbConfig.KeyBufferConfig = new BytesBufferConfig(10, true);
			defaultDbConfig.ValueBufferConfig = new BytesBufferConfig(20, true);
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(1.AsKey(), 2.AsKey());
				var singleItem = db.Query(Range.Prefix(1.AsKey()), Direction.Ascending, 0, int.MaxValue)
					.ToList(x => x != null ? new {Key = x.Key.GetByteArray(true), Value = x.Value.GetByteArray(true)} : null)
					.Single();
				Assert.That(singleItem.Key, Is.EqualTo(1.AsKey()));
				Assert.That(singleItem.Value, Is.EqualTo(2.AsKey()));
			}
		}

		[Test]
		public void ValueBufferIsFixed_TryToAddLargerBytes_CorrectException()
		{
			defaultDbConfig.ValueBufferConfig = new BytesBufferConfig(4, true);
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				var localDb = db;
				var error = Assert.Throws<BdbException>(() => localDb.Add(new byte[] { 1, 2, 33 }, new byte[] { 1, 111, 2, 3, 4 }));
				Assert.That(error.Message, Is.EqualTo(string.Format("requested length [5] is greater than size [4] of fixed buffer, values, database (file name [{0}], database name [testDb])", fileFullPath)));
			}
		}

		[Test]
		public void BytesBufferIsFixed_TryToGrow_CorrectException()
		{
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			{
				using (var db = env.AttachDatabase(defaultDbConfig))
					db.Add(new byte[] { 1, 2, 3, 4, 5 }, new byte[] { 1 });
				defaultDbConfig.KeyBufferConfig = new BytesBufferConfig(4, true);
				using (var db = env.AttachDatabase(defaultDbConfig))
				using (var reader = db.QueryAll())
				{
					var localReader = reader;
					BytesRecord _;
					var error = Assert.Throws<BdbException>(() => localReader.Read(out _));
					Assert.That(error.Message, Is.EqualTo(string.Format("requested length [5] is greater than size [4] of fixed buffer, keys, database (file name [{0}], database name [testDb])", fileFullPath)));
				}
			}
		}

		[Test]
		public void BytesBufferCanGrow()
		{
			defaultDbConfig.KeyBufferConfig = new BytesBufferConfig(1, false);
			defaultDbConfig.ValueBufferConfig = new BytesBufferConfig(2, false);
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add("1", "v1")
					.Add("11", "v2")
					.Add("12", "v2")
					.Add("13", "v22");
				using (var reader = db.QueryByPrefix(Bytes("1")))
				{
					reader.AssertRead("1", "v1");
					moqLogger.Verify(x => x.Warn(It.IsAny<string>()), Times.Never());

					reader.AssertRead("11", "v2");
					moqLogger.Verify(x => x.Warn(string.Format("reallocated from [1] to [2], keys, database (file name [{0}], database name [testDb])", fileFullPath)), Times.Once());

					reader.AssertRead("12", "v2");
					moqLogger.Verify(x => x.Warn(It.IsAny<string>()), Times.Once());

					reader.AssertRead("13", "v22");
					moqLogger.Verify(x => x.Warn(string.Format("reallocated from [2] to [3], values, database (file name [{0}], database name [testDb])", fileFullPath)), Times.Once());

					reader.AssertStop();
				}
			}
		}

		[Test]
		public void BufferSmallCanBeThrownUpToFourTimes()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(new byte[] { 1, 0, 0, 0, 0 }, new byte[] { 1, 0, 0, 0, 0 });
				db.Add(new byte[] { 1, 0, 0, 0, 1 }, new byte[] { 1, 0, 0, 0, 0, 0 });
				db.Add(new byte[] { 1, 0, 0, 0, 2 }, new byte[] { 1, 0, 0, 0, 0, 0, 0 });
			}

			defaultDbConfig.KeyBufferConfig = defaultDbConfig.ValueBufferConfig = BytesBufferConfig.GrowFrom(4);
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			using (var cursor = db.Query(Range.PositiveOpenRay(new byte[] { 1, 0, 0, 0, 0 }), Direction.Ascending, 1, -1))
				cursor.AssertRead()
					.AssertStop();
		}

		[Test]
		public void BufferSmallThrownOnSkip()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(new byte[] { 1 }, new byte[] { 1 });
				db.Add("12345", "1");
				db.Add("12346", "1");
			}

			defaultDbConfig.KeyBufferConfig = BytesBufferConfig.GrowFrom(4);
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			using (var cursor = db.Query(Range.PositiveRay(new byte[] { 1 }), Direction.Ascending, 2, -1))
				cursor
					.AssertRead("12346")
					.AssertStop();
		}

		[Test]
		public void Reader_CheckPrefix()
		{
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add("1", "v1")
					.Add("11", "v11")
					.Add("2", "v2");
				using (var reader = db.QueryByPrefix(Bytes("1")))
					reader
						.AssertRead()
						.AssertRead()
						.AssertStop();
			}
		}

		[Test]
		public void PrefixForMaxByte()
		{
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(new byte[] { 0, 255 }, new byte[] { 0 });
				db.Add(new byte[] { 0, 255, 1 }, new byte[] { 1 });
				db.Add(new byte[] { 1, 0 }, new byte[] { 2 });
				using (var reader = db.QueryByPrefix(new byte[] { 0, 255 }))
				{
					BytesRecord record;
					Assert.That(reader.Read(out record));
					Assert.That(record.Value.DangerousBytes[0], Is.EqualTo(0));
					Assert.That(reader.Read(out record));
					Assert.That(record.Value.DangerousBytes[0], Is.EqualTo(1));
					Assert.That(reader.Read(out record), Is.False);
				}
			}
		}

		[Test]
		public void GetAll()
		{
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add("1", "v1")
					.Add("2", "v2");
				using (var reader = db.QueryAll())
					reader
						.AssertRead("1", "v1")
						.AssertRead("2", "v2")
						.AssertStop();
			}
		}

		[Test]
		public void RecnoDisabled_GetRecordsWithNonZeroStartFrom_CorrectException()
		{
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				var localDb = db;
				var error = Assert.Throws<BdbException>(() => localDb.QueryByPrefix(new byte[0], 1));
				Assert.That(error.Message, Is.EqualTo(string.Format("Bdb was not configured to support record numbers, database (file name [{0}], database name [testDb])", fileFullPath)));
			}
		}

		[Test]
		public void DisposeReaderAfterDbDispose_CorrectExceptionLogged()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			{
				IForwardReader<BytesRecord> reader;
				using (var db = env.AttachDatabase(defaultDbConfig))
					reader = db.QueryByPrefix(new byte[0], 1);
				reader.Dispose();
				Predicate<Exception> checkException = exception => exception is ObjectDisposedException &&
																	exception.Message.Contains(string.Format("database (file name [{0}], database name [testDb])", fileFullPath)) &&
																	exception.StackTrace.Contains("AbstractCursor<NativeRangeCursorReader>.Close()");
				moqLogger.Verify(x => x.Error(string.Format("dispose exception, cursor for database (file name [{0}], database name [testDb])", fileFullPath), Match.Create(checkException)), Times.Once());
			}
		}

		[Test]
		public void EmptyDatabase()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			using (var reader = db.QueryByPrefix(new byte[0], 1))
				reader.AssertStop();
		}

		[Test]
		public void ReaderFinalizerDoNotThrow()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			{
				var db = env.AttachDatabase(defaultDbConfig);
				var reader = db.QueryByPrefix(new byte[0], 1);
				db.Dispose();
				TestHelpers.CallFinalizer(reader);
				Predicate<Exception> checkException = exception => exception is ObjectDisposedException &&
																	exception.Message.Contains(string.Format("database (file name [{0}], database name [testDb])", fileFullPath)) &&
																	exception.StackTrace.Contains("AbstractCursor<NativeRangeCursorReader>.Close()");
				moqLogger.Verify(x => x.Error(string.Format("finalizer exception, cursor for database (file name [{0}], database name [testDb])", fileFullPath), Match.Create(checkException)), Times.Once());
			}
		}

		[Test]
		public void ReadFromDisposedReader_CorrectException()
		{
			defaultDbConfig.EnableRecno = true;

			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				var reader = db.QueryByPrefix(new byte[0], 1);
				reader.Dispose();
				BytesRecord _;
				var error = Assert.Throws<ObjectDisposedException>(() => reader.Read(out _));
				Assert.That(error.Message, Is.StringContaining(string.Format("cursor for database (file name [{0}], database name [testDb])", fileFullPath)));
			}
		}

		[Test]
		public void DbClosed_Read_CorrectException()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			{
				IForwardReader<BytesRecord> reader;
				using (var db = env.AttachDatabase(defaultDbConfig))
					reader = db.QueryByPrefix(new byte[0], 1);
				BytesRecord _;
				var error = Assert.Throws<ObjectDisposedException>(() => reader.Read(out _));
				Assert.That(error.Message, Is.StringContaining(string.Format("database (file name [{0}], database name [testDb])", fileFullPath)));
			}
		}

		[Test]
		public void CanQueryWithInclusiveRightBorder()
		{
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(1, "v1")
					.Add(2, "v2")
					.Add(3, "v3");
				using (var reader = db.Query(Range.Segment(1.AsKey(), 2.AsKey()), Direction.Ascending, 0, int.MaxValue))
					reader
						.AssertRead(1)
						.AssertRead(2)
						.AssertStop();
			}
		}

		[Test]
		public void CanUseTake()
		{
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(1, "v1")
					.Add(2, "v2")
					.Add(3, "v3");
				using (var reader = db.Query(Range.PositiveRay(1.AsKey()), Direction.Ascending, 0, 1))
					reader
						.AssertRead(1)
						.AssertStop();
			}
		}

		[Test]
		public void CanUseDescendingDirection()
		{
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(1, "v1")
					.Add(2, "v2")
					.Add(3, "v3");
				using (var reader = db.Query(Range.PositiveRay(1.AsKey()), Direction.Descending, 0, int.MaxValue))
					reader
						.AssertRead(3)
						.AssertRead(2)
						.AssertRead(1)
						.AssertStop();
			}
		}

		[Test]
		public void CanUseSkipTakeWithDescending()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(1, "v1")
					.Add(2, "v2")
					.Add(3, "v3")
					.Add(4, "v4")
					.Add(5, "v5")
					.Add(6, "v6");
				using (var reader = db.Query(Range.PositiveRay(2.AsKey()), Direction.Descending, 1, 2))
					reader
						.AssertRead(5)
						.AssertRead(4)
						.AssertStop();
			}
		}
	}
}