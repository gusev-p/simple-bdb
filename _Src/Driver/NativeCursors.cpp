#include "NativeCursors.h"
#include <sstream>

using namespace std;

namespace {
	void CheckApiOk(int resultCode, const char* api) {
		if (resultCode != 0)
			throw NativeBdbApiException(resultCode, api);
	}
	unsigned int LittleEndianBytesToInt32(Byte* bytes) {
		return ((int)bytes[0]) | (((int)bytes[1]) << 8) | (((int)bytes[2]) << 16) | (((int)bytes[3]) << 24);
	}
	void Int32ToLittleEndianBytes(unsigned int source, Byte* bytes) {
		*(unsigned int*)bytes = source;
	}
	bool EqualBytes(DBT& dbt, NativeBoundary& boundary) {
		return dbt.size == boundary.length_ && memcmp(dbt.data, boundary.data_, dbt.size) == 0;
	}
}

NativeCursor::NativeCursor(DB* db) {
	CheckApiOk(db->cursor(db, nullptr, &dbc_, 0), "db.cursor");
	memset(&keyDbt_, 0, sizeof(DBT));
	keyDbt_.flags = DB_DBT_USERMEM;
	memset(&valueDbt_, 0, sizeof(DBT));
	valueDbt_.flags = DB_DBT_USERMEM;
}

NativeCursor::~NativeCursor() {
	CheckApiOk(dbc_->close(dbc_), "db.cursor");
}

int NativeCursor::Get(u_int32_t flags) {
	int resultCode = dbc_->get(dbc_, &keyDbt_, &valueDbt_, flags);
	if (resultCode == DB_BUFFER_SMALL)
		throw NativeBufferSmallException(keyDbt_.size, valueDbt_.size);
	return resultCode;
}

bool NativeCursor::TryMove(u_int32_t flags, const char* api) {
	int resultCode = Get(flags);
	if (resultCode == DB_NOTFOUND)
		return false;
	CheckApiOk(resultCode, api);
	return true;
}

void NativeCursor::LoadCurrent() {
	CheckApiOk(Get(DB_CURRENT), "cursor.get.DB_CURRENT");
}

int NativeCursor::GetCurrentRecordNumber() {
	CheckApiOk(Get(DB_GET_RECNO), "cursor.get.DB_GET_RECNO");
	return LittleEndianBytesToInt32((Byte *)valueDbt_.data);
}

bool NativeCursor::TryMoveTo(Byte* key, int length) {
	if (length > keyDbt_.ulen)
		throw NativeBufferSmallException(length, valueDbt_.ulen);
	keyDbt_.size = length;
	memcpy(keyDbt_.data, key, length);
	return TryMove(DB_SET_RANGE, "cursor.get.DB_SET_RANGE");
}

bool NativeCursor::TryMoveTo(int recordNumber) {
	Int32ToLittleEndianBytes(recordNumber, (Byte *)keyDbt_.data);
	keyDbt_.size = sizeof(int);
	return TryMove(DB_SET_RECNO, "cursor.get.DB_SET_RECNO");
}

bool NativeCursor::TryMoveFirst() {
	return TryMove(DB_FIRST, "cursor.get.DB_FIRST");
}

bool NativeCursor::TryMoveLast() {
	return TryMove(DB_LAST, "cursor.get.DB_LAST");
}

bool NativeCursor::TryMoveNext() {
	return TryMove(DB_NEXT, "cursor.get.DB_NEXT");
}

bool NativeCursor::TryMovePrev() {
	return TryMove(DB_PREV, "cursor.get.DB_PREV");
}

bool NativeCursor::TryMoveBy(int offset) {
	return TryMoveTo(GetCurrentRecordNumber() + offset);
}

NativeRangeCursor::NativeRangeCursor(DB* db, NativeRange& range) :range_(std::move(range)), NativeCursor(db) {
}

bool NativeRangeCursor::Within(NativeBoundary& boundary, int direction) {
	if (boundary.length_ == 0)
		return true;
	int result = memcmp(keyDbt_.data, boundary.data_, min(keyDbt_.size, (u_int32_t)boundary.length_));
	if (result == 0)
		return boundary.inclusive_;
	return direction > 0 ? result < 0 : result > 0;
}

bool NativeRangeCursor::TryMoveToLeftBoundary() {
	if (range_.left_.length_ == 0)
		return TryMoveFirst();
	if (!TryMoveTo(range_.left_.data_, range_.left_.length_))
		return false;
	return !range_.left_.inclusive_ && EqualBytes(keyDbt_, range_.left_) ? TryMoveNext() : true;
}

bool NativeRangeCursor::TryMoveToRightBoundary() {
	if (range_.right_.length_ == 0 || !TryMoveTo(range_.right_.data_, range_.right_.length_))
		return TryMoveLast();
	return range_.right_.inclusive_ && EqualBytes(keyDbt_, range_.right_) ? true : TryMovePrev();
}

bool NativeRangeCursor::WithinLeft() {
	return Within(range_.left_, -1);
}

bool NativeRangeCursor::WithinRight() {
	return Within(range_.right_, 1);
}

NativeRangeCursorReader::NativeRangeCursorReader(DB* db, Byte* leftBytes, int leftLength, bool leftInclusive, Byte* rightBytes, int rightLength, bool rightInclusive, int direction, int skip, int take)
	: direction_(direction), skip_(skip), take_(take), readRecordsCount_(0), state_(NotStarted),
	NativeRangeCursor(db, NativeRange(NativeBoundary(leftLength, leftBytes, leftInclusive), NativeBoundary(rightLength, rightBytes, rightInclusive))) {
}

bool NativeRangeCursorReader::Read(unsigned int& keyLength, unsigned int& valueLength) {
	while (true)
		switch (state_) {
		case NotStarted:
			if (take_ == 0)
				state_ = Finished;
			else if (!DoTryMoveFirst())
				state_ = Finished;
			else if (skip_ > 0)
				state_ = Skip;
			else
				state_ = CheckStop;
			break;
		case Started:
			state_ = DoTryMoveNext() ? CheckStop : Finished;
			break;
		case Skip:
			state_ = DoTryMoveBy(skip_) ? CheckStop : Finished;
			break;
		case CheckStop:
			if (!DoWithin())
				state_ = Finished;
			else {
				readRecordsCount_++;
				keyLength = keyDbt_.size;
				valueLength = valueDbt_.size;
				state_ = take_ > 0 && readRecordsCount_ == take_ ? Finished : Started;
				return true;
			}
			break;
		case Finished:
			return false;
	}
}

void NativeRangeCursorReader::ConnectDbtsTo(Byte* keyBuffer, unsigned int keyLength, Byte* valueBuffer, unsigned int valueLength) {
	keyDbt_.data = keyBuffer;
	keyDbt_.ulen = keyLength;
	valueDbt_.data = valueBuffer;
	valueDbt_.ulen = valueLength;
}

NativeRangeCursorReader::RecordNumberKeeper::RecordNumberKeeper(NativeRangeCursorReader& reader)
	:reader_(reader), recordNumber_(reader.state_ == Started ? reader_.GetCurrentRecordNumber() : 0) {
}

NativeRangeCursorReader::RecordNumberKeeper::~RecordNumberKeeper() {
	if (recordNumber_ == 0)
		return;
	if (reader_.TryMoveTo(recordNumber_))
		return;
	stringstream ss;
	ss << "can't move cursor to record number [" << recordNumber_ << "]";
	throw NativeBdbException(ss.str());
}

unsigned int NativeRangeCursorReader::GetTotalCount() {
	RecordNumberKeeper savePosition(*this);
	if (!TryMoveToLeftBoundary())
		return 0;
	int first = GetCurrentRecordNumber();
	if (!TryMoveToRightBoundary())
		return 0;
	int last = GetCurrentRecordNumber();
	int result = last - first + 1;
	return result < 0 ? 0 : result;
}

bool NativeRangeCursorReader::DoTryMoveFirst() {
	return direction_ > 0 ? TryMoveToLeftBoundary() : TryMoveToRightBoundary();
}

bool NativeRangeCursorReader::DoTryMoveNext() {
	return direction_ > 0 ? TryMoveNext() : TryMovePrev();
}

bool NativeRangeCursorReader::DoTryMoveBy(int offset) {
	return direction_ > 0 ? TryMoveBy(offset) : TryMoveBy(-offset);
}

bool NativeRangeCursorReader::DoWithin() {
	return direction_ > 0 ? WithinRight() : WithinLeft();
}

NativeCursorSuffixComparer::NativeCursorSuffixComparer(unsigned int keySuffixOffset, int direction)
	:keySuffixOffset_(keySuffixOffset), direction_(direction) {
}

bool NativeCursorSuffixComparer::operator()(const NativeRangeCursorReader* a, const NativeRangeCursorReader* b) const {
	return direction_ > 0 ? Compare(a, b) > 0 : Compare(a, b) < 0;
}

int NativeCursorSuffixComparer::Compare(const NativeRangeCursorReader* a, const NativeRangeCursorReader* b) const {
	unsigned int aLength = a->keyDbt_.size <= keySuffixOffset_ ? 0 : a->keyDbt_.size - keySuffixOffset_;
	unsigned int bLength = b->keyDbt_.size <= keySuffixOffset_ ? 0 : b->keyDbt_.size - keySuffixOffset_;
	const Byte* aBytes = (Byte*)a->keyDbt_.data + keySuffixOffset_;
	const Byte* bBytes = (Byte*)b->keyDbt_.data + keySuffixOffset_;
	if (aLength == bLength)
		return CompareBytes(aBytes, bBytes, aLength);
	if (aLength < bLength) {
		int result = CompareBytes(aBytes, bBytes, aLength);
		return result != 0 ? result : -1;
	}
	int result = CompareBytes(aBytes, bBytes, bLength);
	return result != 0 ? result : 1;
}

int NativeCursorSuffixComparer::CompareBytes(const Byte* a, const Byte* b, unsigned int count) const {
	return count == 0 ? 0 : memcmp(a, b, count);
}

template <typename TReader>
NativeReaderFetcher<TReader>::NativeReaderFetcher(TReader& reader, bool needKeys, bool needValues, unsigned int take)
	:reader_(reader), needKeys_(needKeys), needValues_(needValues), storeIndex_(0), positionsIndex_(0), recordsFetched_(0), take_(take) {
}

template <typename TReader>
unsigned int NativeReaderFetcher<TReader>::FetchInto(Byte* store, unsigned int* positions) {
	store_ = store;
	positions_ = positions;
	unsigned int positionsIncrement = needKeys_ && needValues_ ? 4 : 2;
	unsigned int storeIncrement = 0;
	if (needKeys_)
		storeIncrement += reader_.keyDbt_.ulen;
	if (needValues_)
		storeIncrement += reader_.valueDbt_.ulen;
	unsigned int keyLength, valueLength;
	while (recordsFetched_ < take_) {
		if (needKeys_)
			SetStart(reader_.keyDbt_, false);
		if (needValues_)
			SetStart(reader_.valueDbt_, needKeys_);
		if (!reader_.Read(keyLength, valueLength))
			return recordsFetched_;
		if (needKeys_)
			SetSize(false, keyLength);
		if (needValues_)
			SetSize(needKeys_, valueLength);
		positionsIndex_ += positionsIncrement;
		storeIndex_ += storeIncrement;
		recordsFetched_++;
	}
	return recordsFetched_;
}

template <typename TReader>
void NativeReaderFetcher<TReader>::SetStart(DBT& source, bool shifted) {
	unsigned int storeIndex = storeIndex_ + (shifted ? reader_.keyDbt_.ulen : 0);
	source.data = &store_[storeIndex];
	positions_[positionsIndex_ + (shifted ? 2 : 0)] = storeIndex;
}

template <typename TReader>
void NativeReaderFetcher<TReader>::SetSize(bool shifted, unsigned int size) {
	positions_[positionsIndex_ + 1 + (shifted ? 2 : 0)] = size;
}

NativeSuffixMergingRangeCursorReader::NativeSuffixMergingRangeCursorReader(unsigned int keySuffixOffset, bool needKeys, bool needValues, NativeRangeCursorReader** readers, unsigned int readersCount, int direction)
	:needKeys_(needKeys), needValues_(needValues), startedReadersCount_(0), lastReader_(nullptr), readers_(readers), readersCount_(readersCount), ReadersHeap(NativeCursorSuffixComparer(keySuffixOffset, direction)) {
}

NativeSuffixMergingRangeCursorReader::~NativeSuffixMergingRangeCursorReader() {
	for (int i = startedReadersCount_; i < readersCount_; i++)
		delete readers_[i];
	delete[] readers_;
	if (lastReader_ != nullptr)
		delete lastReader_;
	for each (NativeRangeCursorReader* reader in c)
		delete reader;
}

void NativeSuffixMergingRangeCursorReader::CopyDbt(DBT& target, DBT& source, unsigned int& length) {
	length = target.size = source.size;
	memcpy(target.data, source.data, source.size);
}

bool NativeSuffixMergingRangeCursorReader::Read(unsigned int& keyLength, unsigned int& valueLength) {
	if (startedReadersCount_ < readersCount_)
		for (int i = startedReadersCount_; i < readersCount_; i++) {
			TryPush(readers_[i]);
			startedReadersCount_++;
		}
	if (lastReader_ == nullptr) {
		if (empty())
			return false;
		lastReader_ = top();
		pop();
	}
	if (needKeys_)
		CopyDbt(keyDbt_, lastReader_->keyDbt_, keyLength);
	if (needValues_)
		CopyDbt(valueDbt_, lastReader_->valueDbt_, valueLength);
	TryPush(lastReader_);
	lastReader_ = nullptr;
	return true;
}

unsigned int NativeSuffixMergingRangeCursorReader::GetTotalCount() {
	unsigned int result = 0;
	for (int i = 0; i < readersCount_; i++)
		result += readers_[i]->GetTotalCount();
	return result;
}

void NativeSuffixMergingRangeCursorReader::TryPush(NativeRangeCursorReader* reader) {
	unsigned int keyLength, valueLength;
	if (reader->Read(keyLength, valueLength))
		push(reader);
	else
		delete reader;
}

template<typename Iter>
static void ConnectChildrenDbtsTo(Iter first, Iter last, Byte*& keyPtr, unsigned int keyLength, Byte*& valuePtr, unsigned int valueLength) {
	for (Iter it = first; it != last; it++)
		ConnectDbts(*it, keyPtr, keyLength, valuePtr, valueLength);
}

template<typename Iter>
static void LoadCurrent(Iter first, Iter last) {
	for (Iter it = first; it != last; it++)
		(*it)->LoadCurrent();
}

static void ConnectDbts(NativeRangeCursorReader* reader, Byte*& keyPtr, unsigned int keyLength, Byte*& valuePtr, unsigned int valueLength) {
	reader->ConnectDbtsTo(keyPtr, keyLength, valuePtr, valueLength);
	keyPtr += keyLength;
	valuePtr += valueLength;
}

void NativeSuffixMergingRangeCursorReader::ConnectDbtsTo(Byte* keyBuffer, unsigned int keyLength, Byte* valueBuffer, unsigned int valueLength) {
	Byte* keyPtr = keyBuffer;
	Byte* valuePtr = valueBuffer;
	ConnectChildrenDbtsTo(readers_ + startedReadersCount_, readers_ + readersCount_, keyPtr, keyLength, valuePtr, valueLength);
	ConnectChildrenDbtsTo(c.begin(), c.end(), keyPtr, keyLength, valuePtr, valueLength);
	LoadCurrent(c.begin(), c.end());
	if (lastReader_ != nullptr) {
		ConnectDbts(lastReader_, keyPtr, keyLength, valuePtr, valueLength);
		lastReader_->LoadCurrent();
	}
	keyDbt_.data = keyPtr;
	keyDbt_.ulen = keyLength;
	valueDbt_.data = valuePtr;
	valueDbt_.ulen = valueLength;
}

template class NativeReaderFetcher < NativeSuffixMergingRangeCursorReader > ;
template class NativeReaderFetcher < NativeRangeCursorReader > ;