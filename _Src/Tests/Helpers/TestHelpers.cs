using System.Reflection;
using System.Text;
using NUnit.Framework;
using SimpleBdb.Driver;
using SimpleBdb.Extensions;
using SimpleBdb.Utils;

namespace SimpleBdb.Tests.Helpers
{
	public static class TestHelpers
	{
		public static IForwardReader<BytesRecord> QueryByPrefix(this Database database, byte[] prefix, int skip = 0)
		{
			return database.Query(Range.Prefix(prefix), Direction.Ascending, skip, int.MaxValue);
		}

		private static byte[] Bytes(string s)
		{
			return Encoding.UTF8.GetBytes(s);
		}

		public static byte[] AsKey(this int b)
		{
			return new[] { (byte)b };
		}

		public static Database Add(this Database db, string key, string value)
		{
			db.Add(Bytes(key), Bytes(value));
			return db;
		}

		public static Database Add(this Database db, int key, string value)
		{
			db.Add(key.AsKey(), Bytes(value));
			return db;
		}

		public static IForwardReader<BytesRecord> AssertRead(this IForwardReader<BytesRecord> reader)
		{
			BytesRecord _;
			Assert.That(reader.Read(out _));
			return reader;
		}

		public static IForwardReader<T> AssertRead<T>(this IForwardReader<T> reader, T item)
		{
			T content;
			Assert.That(reader.Read(out content));
			Assert.That(content, Is.EqualTo(item));
			return reader;
		}

		public static IForwardReader<BytesRecord> AssertRead(this IForwardReader<BytesRecord> reader, string key)
		{
			BytesRecord record;
			Assert.That(reader.Read(out record));
			Assert.That(record.Key.String(), Is.EqualTo(key));
			return reader;
		}

		public static IForwardReader<BytesRecord> AssertRead(this IForwardReader<BytesRecord> reader, byte key)
		{
			BytesRecord record;
			Assert.That(reader.Read(out record));
			Assert.That(record.Key.Byte(), Is.EqualTo(key));
			return reader;
		}

		public static IForwardReader<BytesRecord> AssertRead(this IForwardReader<BytesRecord> reader, string key, string value)
		{
			BytesRecord record;
			Assert.That(reader.Read(out record));
			Assert.That(record.Key.String(), Is.EqualTo(key));
			Assert.That(record.Value.String(), Is.EqualTo(value));
			return reader;
		}

		public static void AssertStop<T>(this IForwardReader<T> reader)
		{
			T _;
			Assert.That(reader.Read(out _), Is.False);
		}

		public static void CallFinalizer(object target)
		{
			var finalizer = target.GetType().GetMethod("Finalize", BindingFlags.NonPublic | BindingFlags.Instance);
			finalizer.Invoke(target, new object[0]);
		}

		public static string String(this BytesBuffer bytesBuffer)
		{
			return Encoding.UTF8.GetString(bytesBuffer.DangerousBytes, 0, bytesBuffer.Length);
		}

		public static int Byte(this BytesBuffer bytesBuffer)
		{
			Assert.That(bytesBuffer.Length, Is.EqualTo(1));
			return bytesBuffer.DangerousBytes[0];
		}
	}
}