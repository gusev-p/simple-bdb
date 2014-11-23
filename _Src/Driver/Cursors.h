#pragma once

#include "Shared.h"

class NativeRangeCursorReader;
class NativeSuffixMergingRangeCursorReader;
template <typename TReader> class NativeReaderFetcher;

namespace SimpleBdb {
	namespace Driver {

		ref class Database;
		enum class Direction;

		namespace Implementation {

			ref class BufferAllocator;

			template <typename TReader>
			private ref class AbstractCursor : BdbComponent {
			public:
				AbstractCursor(Database^ db, TReader* reader, unsigned int readRetriesCount, unsigned int chunkCount);
				SimpleBdb::Utils::BytesTable^ Fetch(FetchOptions options, unsigned int take);
			internal:
				Database^ db_;
				virtual void CheckOpen() override;
			protected:
				TReader* reader_;
				BufferAllocator^ keyAccessor_;
				BufferAllocator^ valueAccessor_;
				unsigned int readRetriesCount_;
				virtual void Close() override;
			};

			private ref class SimpleCursor : AbstractCursor<NativeRangeCursorReader>, ICursor {
			public:
				SimpleCursor(Database^ db, SimpleBdb::Utils::Range^ range, int direction, unsigned int skip, int take);
				virtual SimpleBdb::Utils::BytesTable^ Fetch(FetchOptions options);
				virtual bool Read(SimpleBdb::Utils::BytesRecord^% result);
				virtual unsigned int GetTotalCount();
			private:
				SimpleBdb::Utils::BytesRecord^ content_;
				unsigned int skip_;
				int take_;
			};

			private ref class SuffixMergingFetcher : AbstractCursor<NativeSuffixMergingRangeCursorReader> {
			public:
				SuffixMergingFetcher(Database^ db, array<SimpleBdb::Utils::Range^>^ ranges, int direction, unsigned int keySuffixOffset, FetchOptions options);
				SimpleBdb::Utils::BytesTable^ Fetch(FetchOptions options, int take);
			private:
				unsigned int GetTotalCount();
			};
		}
	}
}