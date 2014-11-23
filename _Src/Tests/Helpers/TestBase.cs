using System.IO;
using System.Text;
using JetBrains.Annotations;
using Moq;
using NUnit.Framework;
using SimpleBdb.Driver;
using SimpleBdb.Utils;

namespace SimpleBdb.Tests.Helpers
{
	public abstract class TestBase
	{
		protected EnvironmentConfig defaultEnvConfig;
		protected DatabaseConfig defaultDbConfig;
		protected Mock<ILogger> moqLogger;
		protected string fileFullPath;
		protected const int enoent = 2;
		protected const int eacces = 13;
		protected const int einval = 22;

		[SetUp]
		public virtual void SetUp()
		{
			FileTestHelpers.RecreateDirectory("testDirectory");
			defaultEnvConfig = new EnvironmentConfig
			{
				FileName = "testDirectory\\testDbEnv",
				CacheSizeInBytes = 1024*1024,
			};
			defaultDbConfig = new DatabaseConfig {Name = "testDb"};
			moqLogger = new Mock<ILogger>();
			fileFullPath = Path.GetFullPath("testDirectory\\testDbEnv");
		}

		[TearDown]
		public virtual void TearDown()
		{
			FileTestHelpers.SafeDeleteDirectory("testDirectory");
		}

		protected static byte[] Bytes(string s)
		{
			return Encoding.UTF8.GetBytes(s);
		}

		protected static uint GetKeysCount([NotNull] Database database)
		{
			return database.GetStatistics(true).bt_nkeys;
		}
	}
}