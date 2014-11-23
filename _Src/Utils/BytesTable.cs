using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace SimpleBdb.Utils
{
	public class BytesTable
	{
		internal readonly byte[] store;
		internal readonly SegmentPosition[] positions;
		public uint RowsCount { get; private set; }
		public uint ColumnsCount { get; private set; }

		public BytesTable(byte[] store, SegmentPosition[] positions, uint rowsCount, uint columnsCount)
		{
			this.store = store;
			this.positions = positions;
			RowsCount = rowsCount;
			ColumnsCount = columnsCount;
		}

		public BytesSegment GetSegment(uint row, uint column)
		{
			if (row >= RowsCount || column >= ColumnsCount)
			{
				const string messageFormat = "invalid arguments, row [{0}], column [{1}], RowsCount [{2}], ColumnsCount [{3}]";
				throw new InvalidOperationException(string.Format(messageFormat, row, column, RowsCount, ColumnsCount));
			}
			var position = positions[row*ColumnsCount + column];
			return new BytesSegment(store, (int) position.start, (int) position.length);
		}

		[NotNull]
		public List<BytesSegment> GetColumn(uint column)
		{
			return GetColumn(column, r => r);
		}

		[NotNull]
		public List<T> GetColumn<T>(uint column, Func<BytesSegment, T> parser)
		{
			return GetDataInternal(row => parser(GetSegment(row, column)));
		}

		[NotNull]
		public List<T> GetKeysAndValues<T>(Func<BytesSegment, BytesSegment, T> parser)
		{
			return GetDataInternal(row => parser(GetSegment(row, 0), GetSegment(row, 1)));
		}

		[NotNull]
		private List<T> GetDataInternal<T>(Func<uint, T> parser)
		{
			var result = new List<T>((int) RowsCount);
			for (uint i = 0; i < RowsCount; i++)
				result.Add(parser(i));
			return result;
		}
	};
}