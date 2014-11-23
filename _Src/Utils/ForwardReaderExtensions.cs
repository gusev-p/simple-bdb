using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace SimpleBdb.Utils
{
	public static class ForwardReaderExtensions
	{
		[NotNull]
		public static List<TResult> ToList<TSource, TResult>([NotNull] this IForwardReader<TSource> reader,
			[NotNull] Func<TSource, TResult> mapper)
		{
			try
			{
				var result = new List<TResult>();
				TSource content;
				while (reader.Read(out content))
					result.Add(mapper(content));
				return result;
			}
			finally
			{
				reader.Dispose();
			}
		}
	}
}