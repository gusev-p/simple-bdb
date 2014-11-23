#pragma once

#include "Shared.h"
#include <exception>
#include <stdexcept>

namespace SimpleBdb {
	namespace Driver {

		enum class Direction;
		ref class BytesBufferConfig;

		namespace Implementation {

			ref class BufferState;

			private ref class BufferAllocator {
			public:
				BufferAllocator(BufferState^ state, unsigned int chunksCount);
				void EnsureChunkCapacity(int capacity);
				SimpleBdb::Utils::BytesBuffer^ buffer_;
				BufferState^ state_;
				unsigned int chunkSize_;
			private:
				void Allocate(int capacity);
				unsigned int chunksCount_;
			};

			private ref class BufferState {
			public:
				BufferState(System::String^ description, BytesBufferConfig^ config, SimpleBdb::Utils::ILogger^ logger);
				void UpdateLength(int newLengthInBytes);
				void CheckLength(int newLengthInBytes);
				int GetLengthInBytes();
				BytesBufferConfig^ config_;
			private:
				System::String^ description_;
				int lengthInBytes_;
				SimpleBdb::Utils::ILogger^ logger_;
			};

			private ref class TestingEnvironment abstract sealed {
			public:
				static System::Collections::Generic::Queue<System::String^>^ ThrowOnDatabaseClose;
			};

			#define DBT_FOR_BYTES_SEGMENT(name, bytesSegment) \
				int name##Len = bytesSegment.Length; \
				pin_ptr<Byte> name##Ptr; \
				if(name##Len > 0) { \
					array<Byte>^ name##bytes = bytesSegment.DangerousGetBytes(); \
					name##Ptr = &name##bytes[bytesSegment.Offset]; \
				} \
				else \
					name##Ptr = nullptr; \
				DBT name##Dbt; \
				memset(&name##Dbt, 0, sizeof(DBT)); \
				name##Dbt.data = name##Ptr; \
				name##Dbt.size = name##Len

			#define DBT_FOR_BYTE_ACCESSOR(name, accessor) \
				array<Byte>^ name##Bytes = accessor->buffer_->DangerousBytes; \
				pin_ptr<Byte> name##Ptr = &name##Bytes[0]; \
				DBT name##Dbt; \
				memset(&name##Dbt, 0, sizeof(DBT)); \
				name##Dbt.data = name##Ptr; \
				name##Dbt.ulen = accessor->buffer_->DangerousBytes->Length; \
				name##Dbt.flags = DB_DBT_USERMEM
		}
	}
}