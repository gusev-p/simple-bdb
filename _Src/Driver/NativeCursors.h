#include "db.h"
#include <exception>
#include <utility>
#include <string>
#include <queue>
#include <vector>

typedef unsigned char Byte;

class NativeBufferSmallException : public std::exception {
public:
	NativeBufferSmallException(unsigned int keySize, unsigned int valueSize) :keySize_(keySize), valueSize_(valueSize) {
	}
	unsigned int KeySize() const { return keySize_; }
	unsigned int ValueSize() const { return valueSize_; }
private:
	unsigned int keySize_;
	unsigned int valueSize_;
};

class NativeBdbApiException : public std::exception {
public:
	NativeBdbApiException(int errorNumber, const char* api) :errorNumber_(errorNumber), api_(api) {
	}
	int ErrorNumber() const { return errorNumber_; }
	const char* Api() const { return api_; }
private:
	int errorNumber_;
	const char* api_;
};

class NativeBdbException : public std::exception {
public:
	NativeBdbException(const std::string message) :message_(message) {
	}
	const std::string& Message() const { return message_; }
private:
	const std::string message_;
};

class NativeBoundary {
public:
	NativeBoundary() :length_(0), data_(nullptr) {
	}
	NativeBoundary(int length, Byte* data, bool inclusive) :length_(length), data_(length_ == 0 ? nullptr : new Byte[length_]), inclusive_(inclusive) {
		if (data_ != nullptr)
			memcpy(data_, data, length_);
	}
	NativeBoundary(NativeBoundary&& source) {
		move(std::move(source));
	}
	const NativeBoundary& operator=(NativeBoundary&& source) {
		move(std::move(source));
		return *this;
	}
	~NativeBoundary() {
		if (data_ != nullptr) {
			delete data_;
			data_ = nullptr;
		}
	}
	int length_;
	Byte* data_;
	bool inclusive_;
private:
	void move(NativeBoundary&& source) {
		length_ = source.length_;
		data_ = source.data_;
		inclusive_ = source.inclusive_;

		source.data_ = nullptr;
	}
	NativeBoundary(const NativeBoundary& source);
	const NativeBoundary& operator=(NativeBoundary& source);
};

class NativeRange {
public:
	NativeRange(NativeBoundary&& left, NativeBoundary&& right) :left_(std::move(left)), right_(std::move(right)) {
	}
	NativeRange(NativeRange&& source) :left_(std::move(source.left_)), right_(std::move(source.right_)) {
	}
	NativeBoundary left_;
	NativeBoundary right_;
private:
	NativeRange(const NativeRange& source);
	const NativeRange& operator=(NativeRange& source);
};

class NativeCursor {
protected:
	NativeCursor(DB* db);
	virtual ~NativeCursor();
	int GetCurrentRecordNumber();
	void LoadCurrent();
	bool TryMoveFirst();
	bool TryMoveLast();
	bool TryMoveNext();
	bool TryMovePrev();
	bool TryMoveBy(int offset);
	template <typename Iter> friend void LoadCurrent(Iter first, Iter last);
	bool TryMoveTo(Byte* key, int length);
	bool TryMoveTo(int recordNumber);
	DBT keyDbt_;
	DBT valueDbt_;
private:
	bool TryMove(u_int32_t flags, const char* api);
	int Get(u_int32_t flags);
	DBC* dbc_;
};

class NativeRangeCursor : public NativeCursor {
protected:
	NativeRangeCursor(DB* db, NativeRange& range);
	bool TryMoveToLeftBoundary();
	bool TryMoveToRightBoundary();
	bool WithinLeft();
	bool WithinRight();
	NativeRange range_;
private:
	bool Within(NativeBoundary& boundary, int direction);
};

template<typename TReader> class NativeReaderFetcher;

class NativeRangeCursorReader : public NativeRangeCursor {
public:
	NativeRangeCursorReader(DB* db, Byte* leftBytes, int leftLength, bool leftInclusive, Byte* rightBytes, int rightLength, bool rightInclusive, int direction, int skip, int take);
	bool Read(unsigned int& keyLength, unsigned int& valueLength);
	unsigned int GetTotalCount();
	void ConnectDbtsTo(Byte* keyBuffer, unsigned int keyLength, Byte* valueBuffer, unsigned int valueLength);
	int readRecordsCount_;
protected:
	bool DoTryMoveFirst();
	bool DoTryMoveNext();
	bool DoTryMoveBy(int offset);
	bool DoWithin();
private:
	enum State {
		NotStarted,
		Started,
		Finished,
		Skip,
		CheckStop
	};

	int direction_;
	int skip_;
	int take_;
	State state_;

	class RecordNumberKeeper {
	public:
		RecordNumberKeeper(NativeRangeCursorReader& reader);
		~RecordNumberKeeper();
	private:
		NativeRangeCursorReader& reader_;
		int recordNumber_;
	};
	friend class NativeCursorSuffixComparer;
	friend class NativeSuffixMergingRangeCursorReader;
	friend class NativeReaderFetcher<NativeRangeCursorReader>;
};

class NativeCursorSuffixComparer {
public:
	NativeCursorSuffixComparer(unsigned int keySuffixOffset, int direction);
	bool operator()(const NativeRangeCursorReader* a, const NativeRangeCursorReader* b) const;
private:
	unsigned int keySuffixOffset_;
	int direction_;
	inline int CompareBytes(const Byte* a, const Byte* b, unsigned int count) const;
	inline int Compare(const NativeRangeCursorReader* a, const NativeRangeCursorReader* b) const;
};

typedef std::priority_queue<NativeRangeCursorReader*, std::vector<NativeRangeCursorReader*>, NativeCursorSuffixComparer> ReadersHeap;

class NativeSuffixMergingRangeCursorReader: private ReadersHeap {
public:
	NativeSuffixMergingRangeCursorReader(unsigned int keySuffixOffset, bool needKeys, bool needValues, NativeRangeCursorReader** readers, unsigned int readersCount, int direction);
	~NativeSuffixMergingRangeCursorReader();
	bool Read(unsigned int& keyLength, unsigned int& valueLength);
	void ConnectDbtsTo(Byte* keyBuffer, unsigned int keyLength, Byte* valueBuffer, unsigned int valueLength);
	unsigned int GetTotalCount();
private:
	bool needKeys_;
	bool needValues_;
	int startedReadersCount_;
	NativeRangeCursorReader* lastReader_;
	NativeRangeCursorReader** readers_;
	unsigned int readersCount_;
	DBT keyDbt_;
	DBT valueDbt_;
	void CopyDbt(DBT& target, DBT& source, unsigned int& length);
	void TryPush(NativeRangeCursorReader* reader);

	friend class NativeReaderFetcher<NativeSuffixMergingRangeCursorReader>;
};

template <typename TReader>
class NativeReaderFetcher {
public:
	NativeReaderFetcher(TReader& reader, bool needKeys, bool needValues, unsigned int take);
	unsigned int FetchInto(Byte* store, unsigned int* positions);
	inline unsigned int FilledStoreSize() {
		return storeIndex_;
	}
	bool needKeys_;
	bool needValues_;
	unsigned int take_;
private:
	TReader& reader_;
	unsigned int storeIndex_;
	unsigned int positionsIndex_;
	unsigned int recordsFetched_;

	Byte* store_;
	unsigned int* positions_;

	void SetStart(DBT& b, bool shifted);
	void SetSize(bool shifted, unsigned int size);
};