using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using SimpleBdb.Utils;

namespace SimpleBdb.Tests.Helpers
{
	public static class Retry
	{
		public static RetryBuilder ByTimeout(TimeSpan timeout)
		{
			Stopwatch stopwatch = null;
			return new RetryBuilder(() => stopwatch = Stopwatch.StartNew(), () => stopwatch.Elapsed > timeout);
		}

		public static RetryBuilder Infinite()
		{
			return ByCount(int.MaxValue);
		}

		public static RetryBuilder ByCount(int count)
		{
			var remainingRetries = 0;
			return new RetryBuilder(() => remainingRetries = count, () => --remainingRetries == 0);
		}

		public class RetryBuilder
		{
			private readonly Action initAction;
			private readonly Func<bool> checkStop;
			private readonly List<Type> exceptionTypes = new List<Type>();
			private TimeSpan? delay;

			public RetryBuilder(Action initAction, Func<bool> checkStop)
			{
				this.initAction = initAction;
				this.checkStop = checkStop;
			}

			public RetryBuilder WithDelay(TimeSpan theDelay)
			{
				delay = theDelay;
				return this;
			}

			public RetryBuilder ForException<TException>()
			{
				exceptionTypes.Add(typeof(TException));
				return this;
			}

			public RetryBuilder ForExceptions(params Type[] trackedExceptions)
			{
				exceptionTypes.AddRange(trackedExceptions);
				return this;
			}

			private ILogger log;
			private string message;

			public RetryBuilder WithLog(ILogger logger, string theMessage)
			{
				log = logger;
				message = theMessage;
				return this;
			}

			private Action failAction;

			public RetryBuilder WhenFailed(Action action)
			{
				failAction = action;
				return this;
			}

			public Func<Func<T>, T> BuildWithResult<T>()
			{
				var action = Build();
				return delegate(Func<T> func)
				{
					var result = default(T);
					action(() => result = func());
					return result;
				};
			}

			public Action<Action> Build()
			{
				return delegate(Action action)
				{
					initAction();
					while (true)
						try
						{
							action();
							return;
						}
						catch (Exception e)
						{
							if (log != null)
								log.Warn(message + "\r\n" + e);
							if (exceptionTypes.Any() && !exceptionTypes.Any(ex => ex.IsInstanceOfType(e)) || checkStop())
								throw;
							if (failAction != null)
								failAction();
							if (delay.HasValue)
								Thread.Sleep(delay.Value);
						}
				};
			}

			public static implicit operator Action<Action>(RetryBuilder builder)
			{
				return builder.Build();
			}
		}
	}
}