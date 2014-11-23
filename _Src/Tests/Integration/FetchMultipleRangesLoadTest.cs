using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using NUnit.Framework;
using SimpleBdb.Driver;
using SimpleBdb.Extensions;
using SimpleBdb.Tests.Helpers;
using SimpleBdb.Tests.Helpers.BitConversion;
using SimpleBdb.Utils;

namespace SimpleBdb.Tests.Integration
{
	[TestFixture]
	[Category("Manual")]
	public class FetchMultipleRangesLoadTest : TestBase
	{
		private const int totalItemsCount = 10000000;
		private const int itemsPerPrefix = 1000;
		private const int prefixCount = totalItemsCount / itemsPerPrefix;
		private const int takeCount = 100;
		private const int rangesCount = 30;
		private const int keySize = 100;
		private const int valueSize = 20;
		private const int iterationsCount = 10000;
		private const int testCount = 5;
		private const long mb = 1024 * 1024;

		private readonly Random random = new Random();
		private Driver.Environment environment;
		private Database database;
		private PrefixItem[] trash;
		private TestCase[] targetCases;
		//private readonly IComparer<BytesBuffer> comparer = new KeySuffixComparer(4);

		private class TestCase
		{
			public Range[] ranges;
			//			public int[] Indexes;
		}

		//[Test]
		public void Test()
		{
			GenerateTrash();
			SaveTrashToBdb(trash);
			Assert.That(GetKeysCount(database), Is.EqualTo(totalItemsCount));
			PrepareTestRanges();
			//ExecuteTest("slow", DoQuerySlow);
			ExecuteTest("fast", DoQueryFast);
		}

		private void PrepareTestRanges()
		{
			targetCases = Enumerable
				.Range(0, iterationsCount)
				.Select(delegate(int i)
				{
					var prefixIndexes = Enumerable.Range(0, rangesCount).Select(_ => random.Next(trash.Length)).ToArray();
					var ranges = prefixIndexes
						.Select(index => Range.Prefix(trash[index].prefixBytes))
						.ToArray();
					return new TestCase
					{
						//Indexes = prefixIndexes,
						ranges = ranges
					};
				})
				.ToArray();
		}

		private void ExecuteTest(string caption, Func<Range[], List<BytesSegment>> query)
		{
			var times = new List<long>();
			for (var i = 0; i < testCount; i++)
			{
				var stopwatch = Stopwatch.StartNew();
				//if (!PerformanceProfiler.IsActive)
				//throw new ApplicationException("Application isn't running under the profiler");
				//PerformanceProfiler.Begin();
				//PerformanceProfiler.Start();
				foreach (var t in targetCases)
				{
					//AssertEquals(DoEtalonQuery(t.Indexes), query(t.ranges));
					if (query(t.ranges).Count != takeCount)
						Assert.Fail();
				}
				//PerformanceProfiler.Stop();
				//PerformanceProfiler.EndSave();
				stopwatch.Stop();
				times.Add(stopwatch.ElapsedMilliseconds);
				Console.Out.WriteLine("{0} - {1} millis", caption, stopwatch.ElapsedMilliseconds);
			}
			Console.Out.WriteLine("{0} - average {1} millis", caption, times.Average());
		}

		public override void SetUp()
		{
			base.SetUp();
			defaultEnvConfig.CacheSizeInBytes = 64 * mb;
			defaultEnvConfig.IsPersistent = true;
			environment = new Driver.Environment(defaultEnvConfig, moqLogger.Object);
			database = environment.AttachDatabase(new DatabaseConfig
			{
				Name = "loadTest",
				EnableRecno = true,
				CachePriority = CachePriority.VeryHigh,
				KeyBufferConfig = BytesBufferConfig.FixedTo(keySize),
				ValueBufferConfig = BytesBufferConfig.FixedTo(valueSize)
			});
		}

		public override void TearDown()
		{
			database.Dispose();
			environment.Dispose();
			base.TearDown();
		}

		private void FillRandomUnique(byte[] target)
		{
			random.NextBytes(target);
			Guid.NewGuid().ToByteArray().CopyTo(target, 4);
		}

		private void GenerateTrash()
		{
			trash = new PrefixItem[prefixCount];

			for (var i = 0; i < trash.Length; i++)
			{
				var prefix = random.Next();
				var prefixBytes = EndianBitConverter.Big.GetBytes(prefix);
				var items = new Item[itemsPerPrefix];
				for (var j = 0; j < items.Length; j++)
				{
					var key = new byte[keySize];
					FillRandomUnique(key);
					var value = new byte[valueSize];
					FillRandomUnique(value);
					Array.Copy(prefixBytes, key, prefixBytes.Length);
					items[j] = new Item
					{
						key = key,
						value = value
					};
				}
				trash[i] = new PrefixItem
				{
					prefixBytes = prefixBytes,
					items = items
				};
			}
		}

		private void SaveTrashToBdb(IEnumerable<PrefixItem> prefixItems)
		{
			foreach (var t in prefixItems.SelectMany(x => x.items))
				database.Add(t.key, t.value);
		}

		//private static void AssertEquals(byte[][] expected, ByteRange[] actual)
		//{
		//    Assert.That(actual.Length, Is.EqualTo(expected.Length));
		//    for (var i = 0; i < expected.Length; i++)
		//        Assert.That(ByteArrayComparer.Instance.Compare(expected[i], actual[i].ToByteArray()), Is.EqualTo(0));
		//}

		//private ByteRange[] DoQuerySlow(IEnumerable<Range> ranges)
		//{
		//    var result = new ByteRange[takeCount];
		//    using (var reader = ranges.Select(r => database.Query(r, Direction.Ascending, 0, int.MaxValue).Keys()).Merge(comparer).Take(takeCount))
		//    {
		//        var i = 0;
		//        BytesBuffer buffer;
		//        while (reader.Read(out buffer))
		//            result[i++] = buffer.CopyToByteRange();
		//    }
		//    return result;
		//}

		private List<BytesSegment> DoQueryFast(Range[] ranges)
		{
			return database.Fetch(ranges, Direction.Ascending, takeCount, 4, FetchOptions.Keys).GetColumn(0);
		}

		//private byte[][] DoEtalonQuery(IEnumerable<int> indexes)
		//{
		//    return indexes
		//        .SelectMany(index => trash[index].items.Select(x => x.key))
		//        .OrderBy(x => new BytesBuffer { Bytes = x, Length = x.Length }, comparer)
		//        .Take(takeCount)
		//        .ToArray();
		//}

		private class PrefixItem
		{
			public byte[] prefixBytes;
			public Item[] items;
		}

		private class Item
		{
			public byte[] key;
			public byte[] value;
		}

		//public class KeySuffixComparer : IComparer<BytesBuffer>
		//{
		//    private readonly int offset;

		//    public KeySuffixComparer(int offset)
		//    {
		//        this.offset = offset;
		//    }

		//    public int Compare(BytesBuffer x, BytesBuffer y)
		//    {
		//        return GetSuffix(x).CompareTo(GetSuffix(y));
		//    }

		//    private ByteRange GetSuffix(BytesBuffer buffer)
		//    {
		//        return new ByteRange(buffer.Bytes, offset, buffer.Length - offset);
		//    }
		//}
	}
}