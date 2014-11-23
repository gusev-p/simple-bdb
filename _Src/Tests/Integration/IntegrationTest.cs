using System;
using System.Collections.Generic;
using System.Text;
using NUnit.Framework;
using SimpleBdb.Driver;
using SimpleBdb.Extensions;
using SimpleBdb.Tests.Helpers;
using SimpleBdb.Tests.Helpers.BitConversion;
using SimpleBdb.Utils;

namespace SimpleBdb.Tests.Integration
{
	[TestFixture]
	public abstract class IntegrationTest : TestBase
	{
		public class RecordNumbersCalculation : IntegrationTest
		{
			private class TestCase
			{
				public TestCase(int start, string prefix, int shift)
				{
					this.start = start;
					this.prefix = prefix;
					this.shift = shift;
					buffer = new byte[prefix.Length + sizeof(int)];
					Encoding.ASCII.GetBytes(prefix, 0, prefix.Length, buffer, 0);
				}

				public void Insert(Database database, int index)
				{
					var bytes = EndianBitConverter.Big.GetBytes(index);
					Array.Copy(bytes, 0, buffer, buffer.Length - sizeof(int), sizeof(int));
					database.Add(buffer, Bytes("v" + (long)index));
				}

				public void ExecuteAssertion(Database database)
				{
					using (var reader = database.QueryByPrefix(Encoding.ASCII.GetBytes(prefix), shift))
					{
						BytesRecord content;
						Assert.That(reader.Read(out content));
						Assert.That(content.Value.String(), Is.EqualTo("v" + (start + shift)));
					}
				}

				public readonly int start;
				private readonly string prefix;
				private readonly int shift;
				private readonly byte[] buffer;
			}

			private List<TestCase> testCases;

			public override void SetUp()
			{
				base.SetUp();
				testCases = new List<TestCase>();
			}

			private TestCase NearesetTestCase(int index)
			{
				for (var i = testCases.Count - 1; i >= 0; i--)
				{
					var result = testCases[i];
					if (index >= result.start)
						return result;
				}
				throw new InvalidOperationException();
			}

			private void NewTestCase(int start, string prefix, int shift)
			{
				testCases.Add(new TestCase(start, prefix, shift));
			}

			[Test]
			public void Test()
			{
				NewTestCase(0, "", 67000);
				NewTestCase(71, "a", 40);
				NewTestCase(217, "ab", 300);
				NewTestCase(65530, "abc", 350);

				defaultDbConfig.EnableRecno = true;
				defaultEnvConfig.IsPersistent = true;
				using (var env = new Driver.Environment(defaultEnvConfig, moqLogger.Object))
				using (var db = env.AttachDatabase(defaultDbConfig))
					for (var i = 0; i < 100000; i++)
						NearesetTestCase(i).Insert(db, i);

				using (var env = new Driver.Environment(defaultEnvConfig, moqLogger.Object))
				using (var db = env.AttachDatabase(defaultDbConfig))
				{
					Assert.That(GetKeysCount(db), Is.EqualTo(100000));
					foreach (var testCase in testCases)
						testCase.ExecuteAssertion(db);
				}
			}
		}
	}
}