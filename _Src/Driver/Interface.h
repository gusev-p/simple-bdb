#pragma once

#include "Shared.h"

namespace SimpleBdb {
	namespace Driver {
		namespace Implementation {
			ref class BufferState;
			ref class BufferAllocator;
		}

		public ref struct EnvironmentConfig {
			System::String^ FileName;
			long long CacheSizeInBytes;
			bool IsPersistent;
		};

		public enum class CachePriority {
			Unchanged = 0,
			VeryLow = 1,
			Low = 2,
			Default = 3,
			High = 4,
			VeryHigh = 5
		};

		public enum class Direction
		{
			Ascending = 0,
			Descending = 1,
		};

		public ref class BytesBufferConfig {
		public:
			static BytesBufferConfig^ FixedTo(int size) { return gcnew BytesBufferConfig(max(size, minFixedSize), true); }
			static BytesBufferConfig^ GrowFrom(int size) { return gcnew BytesBufferConfig(max(size, minFixedSize), false); }
			static BytesBufferConfig^ MinFixed() { return FixedTo(minFixedSize); }
		internal:
			BytesBufferConfig(int size, bool fixed) :Size(size), Fixed(fixed) {
			}
			int Size;
			bool Fixed;
		private:
			static int minFixedSize = sizeof(unsigned int);
		};

		public ref struct DatabaseConfig {
			DatabaseConfig() :KeyBufferConfig(BytesBufferConfig::GrowFrom(100)), ValueBufferConfig(BytesBufferConfig::GrowFrom(100)) {
			}
			System::String^ Name;
			CachePriority CachePriority;
			bool EnableRecno;
			bool IsReadonly;
			BytesBufferConfig^ KeyBufferConfig;
			BytesBufferConfig^ ValueBufferConfig;
		};

		ref class Database;

		public ref class Environment : public Implementation::BdbComponent {
		public:
			Environment([NotNull] EnvironmentConfig^ config, [NotNull] SimpleBdb::Utils::ILogger^ logger);

			//CDS is not used because we need atomic operations over multiple databases
			//(index/inverted index). Client is responsible for locking,
			//environment is only the container for single common RW-lock.
			[NotNull]
			property System::Threading::ReaderWriterLockSlim^ Locker {
				System::Threading::ReaderWriterLockSlim^ get(){ return locker_; }
			}

			[NotNull]
			property EnvironmentConfig^ Config {
				EnvironmentConfig^ get() { return config_; }
			}
			[NotNull]
			property System::Collections::Generic::IEnumerable<Database^>^ Databases {
				System::Collections::Generic::IEnumerable<Database^>^ get() {
					return databases_;
				}
			}
			[NotNull]
			System::String^ DumpStats();
			[NotNull] Database^ AttachDatabase([NotNull] DatabaseConfig^ config);
		internal:
			void LogErrorViaBdb(int error, System::String^ message);
			void TrackDatabase(Database^ database);
			void UntrackDatabase(Database^ database);

			DB_ENV* dbEnv_;
			EnvironmentConfig^ config_;
			System::String^ fileName_;
		protected:
			virtual void Close() override;
		private:
			System::Collections::Generic::List<Database^>^ databases_;
			System::Threading::ReaderWriterLockSlim^ locker_;
		};

		[System::Runtime::InteropServices::StructLayout(System::Runtime::InteropServices::LayoutKind::Sequential, CharSet = System::Runtime::InteropServices::CharSet::Ansi, Pack = 1)]
		public value struct DatabaseStatistics
		{
			unsigned int bt_magic;
			unsigned int bt_version;
			unsigned int bt_metaflags;
			unsigned int bt_nkeys;
			unsigned int bt_ndata;
			unsigned int bt_pagecnt;
			unsigned int bt_pagesize;
			unsigned int bt_minkey;
			unsigned int bt_nblobs;
			unsigned int bt_re_len;
			unsigned int bt_re_pad;
			unsigned int bt_levels;
			unsigned int bt_int_pg;
			unsigned int bt_leaf_pg;
			unsigned int bt_dup_pg;
			unsigned int bt_over_pg;
			unsigned int bt_empty_pg;
			unsigned int bt_free;
			unsigned long long bt_int_pgfree;
			unsigned long long bt_leaf_pgfree;
			unsigned long long bt_dup_pgfree;
			unsigned long long bt_over_pgfree;
		};

		public ref class Database : public Implementation::BdbComponent {
		public:
			void Add(SimpleBdb::Utils::BytesSegment key, SimpleBdb::Utils::BytesSegment value);
			void Remove(SimpleBdb::Utils::BytesSegment key);
			[CanBeNull] SimpleBdb::Utils::BytesBuffer^ Find(SimpleBdb::Utils::BytesSegment key);
			[NotNull] SimpleBdb::Driver::ICursor^ Query([NotNull] SimpleBdb::Utils::Range^ range, Direction direction, int skip, int take);
			[NotNull] SimpleBdb::Utils::BytesTable^ Fetch([NotNull] array<SimpleBdb::Utils::Range^>^ ranges, Direction direction, int take, unsigned int keySuffixOffset, FetchOptions options);
			[NotNull] DatabaseStatistics GetStatistics(bool fast);
			[NotNull]
			property DatabaseConfig^ Config {
				DatabaseConfig^ get() { return config_; }
			}
		internal:
			Database([NotNull] Environment^ env, [NotNull] DatabaseConfig^ config);
			void Open();
			void LogErrorViaBdb(int error, System::String^ message);
			void CheckRecordNumbersEnabled();

			DB* db_;
			Environment^ env_;
			DatabaseConfig^ config_;
			Implementation::BufferState^ keysState_;
			Implementation::BufferState^ valuesState_;
		protected:
			virtual void Close() override;
		private:
			int DoFind(SimpleBdb::Utils::BytesSegment key, Implementation::BufferAllocator^ valueAccessor);
		};

		public ref class BdbException : System::Exception {
		public:
			BdbException(System::String^ message) : Exception(message) {
			}
		};

		public ref class BdbApiException : BdbException {
		public:
			BdbApiException(System::String^ message, int errorNumber) : BdbException(message), errorNumber_(errorNumber) {
			}
			property int ErrorNumber {
				int get() { return errorNumber_; }
			}
		private:
			int errorNumber_;
		};
	}
}