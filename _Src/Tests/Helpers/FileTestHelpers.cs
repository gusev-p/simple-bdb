using System;
using System.IO;

namespace SimpleBdb.Tests.Helpers
{
	public static class FileTestHelpers
	{
		public static void RecreateDirectory(string directory)
		{
			SafeDeleteDirectory(directory);
			Directory.CreateDirectory(directory);
		}

		public static void SafeDeleteDirectory(string directory)
		{
			ExecuteWithRetry(delegate
			{
				if (Directory.Exists(directory))
					Directory.Delete(directory, true);
			}, "error deleting directory [{0}]", directory);
		}

		public static void SafeDeleteFile(string file)
		{
			ExecuteWithRetry(delegate
			{
				if (File.Exists(file))
					File.Delete(file);
			}, "error deleting file [{0}]", file);
		}

		private static void ExecuteWithRetry(Action action, string message, params object[] args)
		{
			Retry.ByCount(10)
				.ForException<IOException>()
				.WithDelay(TimeSpan.FromMilliseconds(100))
				.WithTestingLogging(string.Format(message, args))
				.Build()
				.Invoke(action);
		}

		public static Retry.RetryBuilder WithTestingLogging(this Retry.RetryBuilder retryBuilder, string message)
		{
			return retryBuilder.WithLog(new ConsoleLoggerForTests(), message);
		}
	}
}