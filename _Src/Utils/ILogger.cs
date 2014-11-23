using System;

namespace SimpleBdb.Utils
{
	public interface ILogger
	{
		void Error(string message);
		void Error(string message, Exception exception);
		void Warn(string message);
	}
}