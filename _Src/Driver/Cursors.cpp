#include "Stdafx.h"
#include "Cursors.h"
#include "Interface.h"
#include "Implementation.h"
#include "NativeCursors.h"

using namespace SimpleBdb::Driver::Implementation;

using System::String;
using System::Collections::Generic::IEnumerable;
using System::Linq::Enumerable;
using SimpleBdb::Utils::Range;
using SimpleBdb::Utils::BytesRecord;
using SimpleBdb::Driver::Byte;
using SimpleBdb::Driver::Direction;
using SimpleBdb::Driver::Database;
using SimpleBdb::Driver::ICursor;
using SimpleBdb::Driver::FetchOptions;
using SimpleBdb::Utils::BytesTable;
using SimpleBdb::Utils::SegmentPosition;

#define INVOKE_NATIVE(f, retriesCount) \
	array<Byte>^ keyBytes = keyAccessor_->buffer_->DangerousBytes; \
	pin_ptr<Byte> keyPtr = &keyBytes[0]; \
	array<Byte>^ valueBytes = valueAccessor_->buffer_->DangerousBytes; \
	pin_ptr<Byte> valuePtr = &valueBytes[0]; \
	for(int __rep = 0; __rep < retriesCount; __rep++) \
		try { \
			reader_->ConnectDbtsTo(keyPtr, keyAccessor_->chunkSize_, valuePtr, valueAccessor_->chunkSize_); \
			f; \
		} \
		catch(const NativeBufferSmallException& e) { \
			keyAccessor_->EnsureChunkCapacity(e.KeySize()); \
			valueAccessor_->EnsureChunkCapacity(e.ValueSize()); \
			keyBytes = keyAccessor_->buffer_->DangerousBytes; \
			keyPtr = &keyBytes[0]; \
			valueBytes = valueAccessor_->buffer_->DangerousBytes; \
			valuePtr = &valueBytes[0]; \
		} \
		catch(const NativeBdbApiException& e) { \
			throw gcnew BdbApiException(FormatApiMessage(e.ErrorNumber(), gcnew String(e.Api())), e.ErrorNumber()); \
		} \
		catch (const NativeBdbException& e) { \
			throw gcnew BdbException(gcnew String(e.Message().c_str()) + ", " + description_); \
		} \
	throw gcnew BdbException("INVOKE_NATIVE assertion failure, " + description_); \

template <typename TReader>
AbstractCursor<TReader>::AbstractCursor(Database^ db, TReader* reader, unsigned int readRetriesCount, unsigned int chunksCount)
	:db_(db), reader_(reader),
	keyAccessor_(gcnew BufferAllocator(db->keysState_, chunksCount)),
	valueAccessor_(gcnew BufferAllocator(db->valuesState_, chunksCount)), readRetriesCount_(readRetriesCount),
	BdbComponent(db_->logger_, "cursor for " + db_->description_) {
}

template <typename TReader>
void AbstractCursor<TReader>::CheckOpen() {
	BdbComponent::CheckOpen();
	db_->CheckOpen();
}

template <typename TReader>
void AbstractCursor<TReader>::Close() {
	CheckOpen();
	delete reader_;
	reader_ = nullptr;
}

template <typename TReader>
BytesTable^ AbstractCursor<TReader>::Fetch(FetchOptions options, unsigned int take) {
	CheckOpen();
	bool needKeys = options == FetchOptions::Keys || options == FetchOptions::KeysAndValues;
	bool needValues = options == FetchOptions::Values || options == FetchOptions::KeysAndValues;
	unsigned int columnsCount = 0;
	if (needKeys)
		columnsCount++;
	if (needValues)
		columnsCount++;
	array<Byte>^ store = nullptr;
	array<SegmentPosition>^ positions = gcnew array<SegmentPosition>(columnsCount * take);
	unsigned int rowsCount = 0;
	if (take == 0)
		return gcnew BytesTable(store, positions, rowsCount, columnsCount);
	NativeReaderFetcher<TReader> fetcher(*reader_, needKeys, needValues, take);
	INVOKE_NATIVE({
		unsigned int storeSize = 0;
		if (needKeys)
			storeSize += keyAccessor_->chunkSize_;
		if (needValues)
			storeSize += valueAccessor_->chunkSize_;
		storeSize *= take;
		array<Byte>^ newStore = gcnew array<Byte>(storeSize);
		if (store != nullptr) {
			pin_ptr<Byte> oldStorePtr = &store[0];
			pin_ptr<Byte> newStorePtr = &newStore[0];
			memcpy(newStorePtr, oldStorePtr, fetcher.FilledStoreSize());
		}
		store = newStore;
		pin_ptr<Byte> storePtr = &newStore[0];
		pin_ptr<SegmentPosition> positionsPtr = &positions[0];
		rowsCount = fetcher.FetchInto(storePtr, (unsigned int *)positionsPtr);
		return gcnew BytesTable(store, positions, rowsCount, columnsCount);
	}, (take + 1) * readRetriesCount_ * 10);
}

#define DECLARE_NATIVE_BOUNDARY(name, boundary) \
	int name##Length; \
	pin_ptr<Byte> name##Ptr; \
	bool name##Inclusive; \
	if(boundary != nullptr) {\
		array<Byte>^ name##Bytes = boundary->Value; \
		name##Length = name##Bytes->Length; \
		name##Inclusive = boundary->Inclusive; \
		if(name##Length > 0) \
			name##Ptr = &name##Bytes[0]; \
	} \
	else { \
		name##Length = 0; \
		name##Ptr = nullptr; \
		name##Inclusive = false; \
	}

static NativeRangeCursorReader* CreateNativeRangeCursorReader(Database^ db, Range^ range, int direction, unsigned int skip, unsigned int take) {
	DECLARE_NATIVE_BOUNDARY(left, range->Left);
	DECLARE_NATIVE_BOUNDARY(right, range->Right);
	return new NativeRangeCursorReader(db->db_, leftPtr, leftLength, leftInclusive, rightPtr, rightLength, rightInclusive, direction, skip, take);
}

SimpleCursor::SimpleCursor(Database^ db, Range^ range, int direction, unsigned int skip, int take)
	:skip_(skip), take_(take), AbstractCursor(db, CreateNativeRangeCursorReader(db, range, direction, skip, take), 5, 1) {
	content_ = gcnew BytesRecord(keyAccessor_->buffer_, valueAccessor_->buffer_);
}

bool SimpleCursor::Read(BytesRecord^% result) {
	CheckOpen();
	unsigned int keyLength, valueLength;
	INVOKE_NATIVE(
		if (reader_->Read(keyLength, valueLength)) {
		keyAccessor_->buffer_->Length = keyLength;
		valueAccessor_->buffer_->Length = valueLength;
		result = content_;
		return true;
		}
		else {
			result = nullptr;
			return false;
		}, readRetriesCount_);
}

BytesTable^ SimpleCursor::Fetch(FetchOptions options) {
	int recordsCount = take_ >= 0 && take_ < System::Int32::MaxValue
		? take_ - reader_->readRecordsCount_
		: GetTotalCount() - skip_ - reader_->readRecordsCount_;
	return AbstractCursor::Fetch(options, recordsCount);
}

unsigned int SimpleCursor::GetTotalCount() {
	CheckOpen();
	db_->CheckRecordNumbersEnabled();
	INVOKE_NATIVE(return reader_->GetTotalCount(); , 7)
}

static NativeSuffixMergingRangeCursorReader* CreateNativeSuffixMergingRangeCursorReader(Database^ db, array<Range^>^ ranges, int direction, unsigned int keySuffixOffset, FetchOptions options) {
	NativeRangeCursorReader** readers = new NativeRangeCursorReader*[ranges->Length];
	for (int i = 0; i < ranges->Length; i++)
		readers[i] = CreateNativeRangeCursorReader(db, ranges[i], direction, 0, -1);
	bool needKeys = options == FetchOptions::Keys || options == FetchOptions::KeysAndValues;
	bool needValues = options == FetchOptions::Values || options == FetchOptions::KeysAndValues;
	return new NativeSuffixMergingRangeCursorReader(keySuffixOffset, needKeys, needValues, readers, ranges->Length, direction);
}

SuffixMergingFetcher::SuffixMergingFetcher(Database^ db, array<Range^>^ ranges, int direction, unsigned int keySuffixOffset, FetchOptions options)
	:AbstractCursor(db, CreateNativeSuffixMergingRangeCursorReader(db, ranges, direction, keySuffixOffset, options),
	5 * ranges->Length, ranges->Length + 1) {
}

//todo pre release hacks, make it right

BytesTable^ SuffixMergingFetcher::Fetch(FetchOptions options, int take) {
	int recordsCount = take < 0 || take == System::Int32::MaxValue ? GetTotalCount() : take;
	return AbstractCursor::Fetch(options, recordsCount);
}

unsigned int SuffixMergingFetcher::GetTotalCount() {
	CheckOpen();
	db_->CheckRecordNumbersEnabled();
	INVOKE_NATIVE(return reader_->GetTotalCount(); , readRetriesCount_ * 10)
}