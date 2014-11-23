using System.Linq;
using JetBrains.Annotations;
using SimpleBdb.Driver;

namespace SimpleBdb.Extensions
{
	public static class EnvironmentExtensions
	{
		[NotNull]
		public static Database GetDatabaseByName([NotNull] this Environment environment, [NotNull] string name)
		{
			return environment.Databases.Single(x => x.Config.Name == name);
		}
	}
}