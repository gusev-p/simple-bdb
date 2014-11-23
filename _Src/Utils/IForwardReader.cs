using System;

namespace SimpleBdb.Utils
{
	public interface IForwardReader<TContent> : IDisposable
	{
		bool Read(out TContent content);
	}
}