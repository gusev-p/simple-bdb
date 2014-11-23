using System;
using SimpleBdb.Utils;

namespace SimpleBdb.Tests.Helpers
{
	public class ConsoleLoggerForTests : ILogger
	{
		public void Error(string message)
		{
			Console.Out.WriteLine("error " + message);
		}

		public void Error(string message, Exception exception)
		{
			Console.Out.WriteLine("error " + message + "\r\n" + exception);
		}

		public void Warn(string message)
		{
			Console.Out.WriteLine("warn " + message);
		}
	}
}