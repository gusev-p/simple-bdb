using System.Text;
using NUnit.Framework;
using SimpleBdb.Driver;
using SimpleBdb.Extensions;
using SimpleBdb.Tests.Helpers;
using SimpleBdb.Utils;
using TestBase = SimpleBdb.Tests.Helpers.TestBase;

namespace SimpleBdb.Tests
{
	public class ApiGetCountTest : TestBase
	{
		[Test]
		public void GetCountSimple()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(1, "v1").Add(2, "v2").Add(3, "v3");
				Assert.That(db.GetCount(Range.Segment(1.AsKey(), 3.AsKey())), Is.EqualTo(3));
			}
		}

		[Test]
		public void LeftBorderIsGreaterThanBiggestKey_GetCount_ReturnsZero()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(1, "v1").Add(2, "v2").Add(3, "v3");
				Assert.That(db.GetCount(Range.Segment(4.AsKey(), 5.AsKey())), Is.EqualTo(0));
			}
		}

		[Test]
		public void RightBorderIsGreaterThanBiggestKey_GetCount_CountToBiggest()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(1, "v1").Add(2, "v2").Add(3, "v3");
				Assert.That(db.GetCount(Range.Segment(2.AsKey(), 10.AsKey())), Is.EqualTo(2));
			}
		}

		[Test]
		public void LeftGreaterThanRight_GetCount_ReturnsZero()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(1, "v1").Add(2, "v2").Add(3, "v3");
				Assert.That(db.GetCount(Range.Segment(3.AsKey(), 1.AsKey())), Is.EqualTo(0));
			}
		}

		[Test]
		public void GetCountForPositiveRay()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(1, "v1").Add(2, "v2").Add(3, "v3");
				Assert.That(db.GetCount(Range.PositiveRay(3.AsKey())), Is.EqualTo(1));
			}
		}

		[Test]
		public void LeftOpenInterval()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(1, "v1").Add(2, "v2").Add(3, "v3");
				Assert.That(db.GetCount(Range.LeftOpenSegment(2.AsKey(), 3.AsKey())), Is.EqualTo(1));
			}
		}

		[Test]
		public void LeftOpenInterval_LeftBoundaryNotEqual()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(1, "v1").Add(3, "v3").Add(4, "v4");
				Assert.That(db.GetCount(Range.LeftOpenSegment(2.AsKey(), 4.AsKey())), Is.EqualTo(2));
			}
		}

		[Test]
		public void LeftOpenInterval_LeftBoundaryAtTheBiggestKey()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(1, "v1").Add(2, "v2").Add(3, "v3");
				Assert.That(db.GetCount(Range.LeftOpenSegment(3.AsKey(), 4.AsKey())), Is.EqualTo(0));
			}
		}

		[Test]
		public void RightOpenInterval()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(1, "v1").Add(2, "v2").Add(3, "v3");
				Assert.That(db.GetCount(Range.RightOpenSegment(1.AsKey(), 3.AsKey())), Is.EqualTo(2));
			}
		}

		[Test]
		public void RightOpenInterval_RightBoundaryAtTheSmallestKey()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(2, "v2").Add(3, "v3");
				Assert.That(db.GetCount(Range.RightOpenSegment(1.AsKey(), 2.AsKey())), Is.EqualTo(0));
			}
		}

		[Test]
		public void DoNotIncludeItemsGreaterThanRightBorder()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(2, "v2");
				Assert.That(db.GetCount(Range.Segment(1.AsKey(), 1.AsKey())), Is.EqualTo(0));
			}
		}

		[Test]
		public void ByPrefix()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add("11", "v11").Add("21", "v21").Add("31", "v31").Add("15", "v15").Add("27", "v27").Add("12", "v12");
				Assert.That(db.GetCount(Range.Prefix(Encoding.ASCII.GetBytes("1"))), Is.EqualTo(3));
			}
		}

		[Test]
		public void Prefix_ExcludeRightBound()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(new byte[] { 1 }, new byte[] { 100 });
				db.Add(new byte[] { 1, 1 }, new byte[] { 101 });
				db.Add(new byte[] { 1, 2 }, new byte[] { 102 });
				db.Add(new byte[] { 1, 11 }, new byte[] { 102 });
				db.Add(new byte[] { 2 }, new byte[] { 103 });
				Assert.That(db.GetCount(Range.Prefix(new byte[] { 1 })), Is.EqualTo(4));
			}
		}

		[Test]
		public void RecnoDisabled_GetCount_CorrectException()
		{
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				var localDb = db;
				var error = Assert.Throws<BdbException>(() => localDb.GetCount(Range.Prefix(new byte[0])));
				Assert.That(error.Message, Is.EqualTo(string.Format("Bdb was not configured to support record numbers, database (file name [{0}], database name [testDb])",fileFullPath)));
			}
		}

		[Test]
		public void EmptyDatabase()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
				Assert.That(db.GetCount(Range.PositiveRay(new byte[0])), Is.EqualTo(0));
		}

		[Test]
		public void KeyFixedBufferCannotBeSmallerThan4Bytes()
		{
			defaultDbConfig.KeyBufferConfig = new BytesBufferConfig(3, true);
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			{
				var localEnv = env;
				var error = Assert.Throws<BdbException>(() => localEnv.AttachDatabase(defaultDbConfig));
				Assert.That(error.Message, Is.EqualTo(string.Format("fixed buffer size [3] can't be smaller than size of unsigned int [4], keys, database (file name [{0}], database name [testDb])", fileFullPath)));
			}
		}

		[Test]
		public void ValueFixedBufferCannotBeSmallerThan4Bytes()
		{
			defaultDbConfig.ValueBufferConfig = new BytesBufferConfig(3, true);
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			{
				var localEnv = env;
				var error = Assert.Throws<BdbException>(() => localEnv.AttachDatabase(defaultDbConfig));
				Assert.That(error.Message, Is.EqualTo(string.Format("fixed buffer size [3] can't be smaller than size of unsigned int [4], values, database (file name [{0}], database name [testDb])",fileFullPath)));
			}
		}

		[Test]
		public void GetTotalCountPreservesCursorPosition()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(1, "v1").Add(2, "v2").Add(3, "v3");
				using (var cursor = db.Query(Range.Line(), Direction.Ascending, 0, -1))
				{
					cursor.AssertRead(1);
					Assert.That(cursor.GetTotalCount(), Is.EqualTo(3));
					cursor.AssertRead(2).AssertRead(3).AssertStop();
				}
			}
		}

		[Test]
		public void CanReadAfterGetTotalCount()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(1, "v1");
				using (var cursor = db.Query(Range.Line(), Direction.Ascending, 0, -1))
				{
					Assert.That(cursor.GetTotalCount(), Is.EqualTo(1));
					cursor.AssertRead(1).AssertStop();
				}
			}
		}

		[Test]
		public void RecordNumberKeeperRestoreSize()
		{
			defaultDbConfig.EnableRecno = true;
			defaultDbConfig.KeyBufferConfig = BytesBufferConfig.GrowFrom(4);
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(1.AsKey(), 2.AsKey());
				db.Add("longkey", "v");
			}
			defaultDbConfig.KeyBufferConfig = BytesBufferConfig.GrowFrom(4);
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			using (var cursor = db.Query(Range.Line(), Direction.Ascending, 0, -1))
			{
				cursor.AssertRead(1);
				Assert.That(cursor.GetTotalCount(), Is.EqualTo(2));
				cursor.AssertRead("longkey").AssertStop();
			}
		}

		[Test]
		public void HandleBufferSmallOnBoundaryValues()
		{
			defaultDbConfig.EnableRecno = true;
			defaultDbConfig.KeyBufferConfig = BytesBufferConfig.GrowFrom(4);
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(1.AsKey(), 2.AsKey());
				db.Add("longkey", "v");
			}
			defaultDbConfig.KeyBufferConfig = BytesBufferConfig.GrowFrom(4);
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			using (var cursor = db.Query(Range.PositiveRay(Encoding.ASCII.GetBytes("longkey")), Direction.Ascending, 0, -1))
			{
				Assert.That(cursor.GetTotalCount(), Is.EqualTo(1));
				cursor
					.AssertRead("longkey")
					.AssertStop();
			}
		}

		[Test]
		public void BufferSmallExceptionCanBeThownUpToSevenTimes()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add(new byte[] { 1, 0, 0, 0, 0 }, new byte[] { 1, 0, 0, 0, 0 });
				db.Add(new byte[] { 1, 0, 0, 0, 1 }, new byte[] { 1, 0, 0, 0, 0, 0 });
				db.Add(new byte[] { 2, 0, 0, 0, 0, 0 }, new byte[] { 2, 0, 0, 0, 0, 0, 0, 0 });
				db.Add(new byte[] { 2, 0, 0, 0, 0, 1 }, new byte[] { 2, 0, 0, 0, 0, 0, 0 });
			}

			defaultDbConfig.KeyBufferConfig = defaultDbConfig.ValueBufferConfig = BytesBufferConfig.GrowFrom(4);
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			using (var cursor = db.Query(Range.Interval(new byte[] { 1, 0, 0, 0, 0 }, new byte[] { 2, 0, 0, 0, 0, 1 }), Direction.Ascending, 0, -1))
				Assert.That(cursor.GetTotalCount(), Is.EqualTo(2));
		}
	}
}