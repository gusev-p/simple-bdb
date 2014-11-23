using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Moq;
using NUnit.Framework;
using SimpleBdb.Driver;
using SimpleBdb.Driver.Implementation;
using SimpleBdb.Tests.Helpers;
using SimpleBdb.Utils;
using DatabaseExtensions = SimpleBdb.Extensions.DatabaseExtensions;
using Environment = SimpleBdb.Driver.Environment;
using Range = SimpleBdb.Utils.Range;

namespace SimpleBdb.Tests
{
	public class ApiTest : TestBase
	{
		[Test]
		public void Simple()
		{
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(new DatabaseConfig {Name = "testDb", EnableRecno = true}))
			{
				db.Add("k", "v");
				Assert.That(GetKeysCount(db), Is.EqualTo(1));
			}
		}

		[Test]
		public void DuplicateKeys_LastWins()
		{
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(new DatabaseConfig {Name = "testDb", EnableRecno = true}))
			{
				db.Add("k", "v1")
					.Add("k", "v2");
				Assert.That(db
					.Query(Range.Line(), Direction.Ascending, 0, -1)
					.Fetch(FetchOptions.Values)
					.GetColumn(0)
					.Select(x => Encoding.ASCII.GetString(x.ToByteArray()))
					.ToArray(), Is.EqualTo(new[] {"v2"}));
			}
		}

		[Test]
		public void CanAddPartial()
		{
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(new DatabaseConfig {Name = "testDb", EnableRecno = true}))
			{
				var keyBytes = new byte[] {1, 2, 3};
				var valueBytes = new byte[] {10, 20, 30};
				db.Add(new BytesSegment(keyBytes, 1, 2), new BytesSegment(valueBytes, 2, 1));
				var keyToFind = new byte[] {1, 12, 13, 2, 3, 16};
				var result = db.Find(new BytesSegment(keyToFind, 3, 2));
				Assert.IsNotNull(result);
				Assert.That(result.Length, Is.EqualTo(1));
				Assert.That(result.DangerousBytes[0], Is.EqualTo(30));
			}
		}

		[Test]
		public void ErrorOpeningDatabase()
		{
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			{
				Directory.Delete("testDirectory", true);
				var localEnv = env;
				var error = Assert.Throws<BdbApiException>(() => localEnv.AttachDatabase(defaultDbConfig));
				Assert.That(error.ErrorNumber, Is.EqualTo(enoent));
				Assert.That(error.Message,
					Is.EqualTo(
						string.Format("api [db.open] failed, error code [{0}], database (file name [{1}], database name [testDb])", enoent,
							fileFullPath)));
			}
		}

		[Test]
		public void CanUseEmptyValues()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				DatabaseExtensions.Add(db, Bytes("k"), new byte[0]);
				Assert.That(GetKeysCount(db), Is.EqualTo(1));
			}
		}

		[Test]
		public void CanUseEmptyKeys()
		{
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				DatabaseExtensions.Add(db, new byte[0], Bytes("v"));
				var reader = db.QueryByPrefix(new byte[0]);
				BytesRecord record;
				Assert.That(reader.Read(out record), Is.True);
				Assert.That(record.Value.String(), Is.EqualTo("v"));
			}
		}

		[Test]
		public void ChangesPersistBetweenEnvironmentReopens()
		{
			defaultEnvConfig.IsPersistent = true;
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
				db.Add("k", "v");
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
				Assert.That(GetKeysCount(db), Is.EqualTo(1));
		}

		[Test]
		public void DatabaseWithoutRecno_OpenWithRecno_BdbReportsCorrectException()
		{
			defaultEnvConfig.IsPersistent = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
				db.Add("k", "v");
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			{
				var localEnv = env;
				var error = Assert.Throws<BdbApiException>(() => localEnv.AttachDatabase(defaultDbConfig));
				Assert.That(error.ErrorNumber, Is.EqualTo(einval));
				var message =
					string.Format("api [db.open] failed, error code [{0}], database (file name [{1}], database name [testDb])", einval,
						fileFullPath);
				Assert.That(error.Message, Is.EqualTo(message));
			}
			moqLogger.Verify(
				x =>
					x.Error(
						string.Format(
							"bdb reported error: dbpfx, BDB1011 testDb: DB_RECNUM specified to open method but not set in database, database (file name [{0}], database name [testDb])",
							fileFullPath)));
		}

		[Test]
		public void DatabaseErrorsArePrettyFormatted()
		{
			defaultEnvConfig.IsPersistent = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (env.AttachDatabase(defaultDbConfig))
			{
			}

			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			{
				defaultDbConfig.IsReadonly = true;
				using (var db = env.AttachDatabase(defaultDbConfig))
				{
					var k = Bytes("k");
					var v = new byte[0];
					var localDb = db;
					var error = Assert.Throws<BdbApiException>(() => DatabaseExtensions.Add(localDb, k, v));
					Assert.That(error.ErrorNumber, Is.EqualTo(eacces));
					Assert.That(error.Message,
						Is.EqualTo(
							string.Format("api [db.put] failed, error code [{0}], database (file name [{1}], database name [testDb])", eacces,
								fileFullPath)));
				}
			}
		}

		[Test]
		public void CheckDatabasesDisposedOnEnvironmentDispose()
		{
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				env.Dispose();
				Assert.That(db.IsDisposed());
			}
		}

		[Test]
		public void GetDatabaseForDisposedEnvironment_CorrectException()
		{
			var env = new Environment(defaultEnvConfig, moqLogger.Object);
			env.Dispose();
			var error = Assert.Throws<ObjectDisposedException>(() => env.AttachDatabase(defaultDbConfig));
			Assert.That(error.Message, Is.StringContaining(string.Format("environment (file name [{0}])", fileFullPath)));
		}

		[Test]
		public void DatabaseCanLogErrors()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
				db.LogErrorViaBdb(enoent, "test error message");
			moqLogger.Verify(
				x =>
					x.Error(
						string.Format(
							"bdb reported error: dbpfx, test error message: No such file or directory, database (file name [{0}], database name [testDb])",
							fileFullPath)),
				Times.Once());
		}

		[Test]
		public void EnvironmentCanLogErrors()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
				env.LogErrorViaBdb(eacces, "test env error message");

			moqLogger.Verify(
				x =>
					x.Error(
						string.Format(
							"bdb reported error: envpfx, test env error message: Permission denied, environment (file name [{0}])",
							fileFullPath)),
				Times.Once());
		}

		[Test]
		public void CanDoubleDispose()
		{
			var env = new Environment(defaultEnvConfig, moqLogger.Object);
			var db = env.AttachDatabase(defaultDbConfig);
			var reader = db.QueryByPrefix(new byte[0]);
			reader.Dispose();
			reader.Dispose();
			db.Dispose();
			db.Dispose();
			env.Dispose();
			env.Dispose();
		}

		[Test]
		public void EnvironmentFinalizerDoNotThrow()
		{
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			{
				TestingEnvironment.ThrowOnDatabaseClose = new Queue<string>();
				TestingEnvironment.ThrowOnDatabaseClose.Enqueue("crash1");
				TestingEnvironment.ThrowOnDatabaseClose.Enqueue("crash2");
				using (env.AttachDatabase(new DatabaseConfig {Name = "testDb1"}))
				using (env.AttachDatabase(new DatabaseConfig {Name = "testDb2"}))
					TestHelpers.CallFinalizer(env);
			}
			Predicate<Exception> checkCrash1 = e => e is BdbException && e.Message == "crash1";
			moqLogger.Verify(
				x => x.Error(string.Format("dispose exception, database (file name [{0}], database name [testDb1])", fileFullPath),
					Match.Create(checkCrash1)), Times.Once());

			Predicate<Exception> checkCrash2 = e => e is BdbException && e.Message == "crash2";
			moqLogger.Verify(
				x => x.Error(string.Format("dispose exception, database (file name [{0}], database name [testDb2])", fileFullPath),
					Match.Create(checkCrash2)), Times.Once());

			Predicate<Exception> isObjectDisposedException = e => e is ObjectDisposedException;
			moqLogger.Verify(
				x => x.Error(string.Format("dispose exception, database (file name [{0}], database name [testDb1])", fileFullPath),
					Match.Create(isObjectDisposedException)), Times.Once());
			moqLogger.Verify(
				x => x.Error(string.Format("dispose exception, database (file name [{0}], database name [testDb2])", fileFullPath),
					Match.Create(isObjectDisposedException)), Times.Once());
		}

		[Test]
		public void Remove()
		{
			defaultDbConfig.EnableRecno = true;
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				db.Add("1", "v1");
				db.Remove(new BytesSegment(Bytes("1")));
				Assert.That(GetKeysCount(db), Is.EqualTo(0));
				db.Remove(new BytesSegment(Bytes("1")));
				Assert.That(GetKeysCount(db), Is.EqualTo(0));
			}
		}

		[Test]
		public void Find()
		{
			defaultDbConfig.ValueBufferConfig = new BytesBufferConfig(1, false);
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			using (var db = env.AttachDatabase(defaultDbConfig))
			{
				Assert.That(db.Find(new BytesSegment(Bytes("k1"))), Is.Null);
				db.Add("k1", "v1");
				Assert.That(Encoding.ASCII.GetString(db.Find(new BytesSegment(Bytes("k1"))).GetByteArray()), Is.EqualTo("v1"));
			}
			moqLogger.Verify(
				x =>
					x.Warn(string.Format("reallocated from [1] to [2], values, database (file name [{0}], database name [testDb])",
						fileFullPath)), Times.Once());
		}

		[Test]
		public void GetDatabaseByName()
		{
			using (var env = new Environment(defaultEnvConfig, moqLogger.Object))
			{
				Assert.That(env.Databases, Is.Empty);
				var db = env.AttachDatabase(defaultDbConfig);
				Assert.That(env.Databases.Single(), Is.SameAs(db));
				db.Dispose();
				Assert.That(env.Databases, Is.Empty);
			}
		}
	}
}