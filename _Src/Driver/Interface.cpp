#include "Stdafx.h"
#include "Interface.h"
#include "Implementation.h"
#include "Cursors.h"
#include <exception>

using namespace SimpleBdb::Driver;

using System::String;
using System::Collections::Generic::List;
using System::Threading::ReaderWriterLockSlim;
using SimpleBdb::Utils::IForwardReader;
using SimpleBdb::Utils::Range;
using SimpleBdb::Utils::ILogger;
using Implementation::TestingEnvironment;
using Implementation::BufferState;
using Implementation::BufferAllocator;
using Implementation::SimpleCursor;
using Implementation::SuffixMergingFetcher;
using SimpleBdb::Utils::BytesSegment;
using SimpleBdb::Utils::BytesBuffer;
using SimpleBdb::Utils::BytesTable;

const long long gb = 1ll * 1024 * 1024 * 1024;

Environment::Environment(EnvironmentConfig^ config, ILogger^ logger)
	:config_(config), databases_(gcnew List<Database^>()), locker_(gcnew ReaderWriterLockSlim()),
	fileName_(System::IO::Path::GetFullPath(config_->FileName)),
	BdbComponent(logger, String::Format("environment (file name [{0}])", fileName_)) {
	DB_ENV* dbEnv;
	CheckApiOk(db_env_create(&dbEnv, 0), "db_env_create");
	dbEnv_ = dbEnv;
	dbEnv_->set_errpfx(dbEnv_, "envpfx");
	dbEnv_->set_errcall(dbEnv_, GetLogFunc());
	CheckApiOk(dbEnv_->set_cachesize(dbEnv_, static_cast<u_int32_t>(config->CacheSizeInBytes / gb), static_cast<u_int32_t>(config->CacheSizeInBytes % gb), 1), "env.set_cachesize");
	CheckApiOk(dbEnv_->open(dbEnv_, nullptr, DB_CREATE | DB_PRIVATE | DB_THREAD | DB_INIT_MPOOL, 0), "env.open");
}

Database^ Environment::AttachDatabase(DatabaseConfig^ config) {
	CheckOpen();
	auto result = gcnew Database(this, config);
	try {
		result->Open();
		TrackDatabase(result);
	}
	catch (...) {
		result->~Database();
		throw;
	}
	return result;
}

String^ Environment::DumpStats() {
	return "not implemented";
}

void Environment::LogErrorViaBdb(int error, String^ message) {
	CheckOpen();
	std::string stdMessage(msclr::interop::marshal_as<std::string>(message));
	dbEnv_->err(dbEnv_, error, stdMessage.c_str());
}

void Environment::TrackDatabase(Database^ database) {
	databases_->Add(database);
}

void Environment::UntrackDatabase(Database^ database) {
	for (int i = databases_->Count - 1; i >= 0; i--)
		if (ReferenceEquals(databases_[i], database))
			databases_->RemoveAt(i);
}

void Environment::Close() {
	List<Database^>^ databasesToDispose = gcnew List<Database^>(databases_);
	for each(Database^ database in databasesToDispose)
		database->~Database();
	CheckApiOk(dbEnv_->close(dbEnv_, 0), "env.close");
	dbEnv_ = nullptr;
}

Database::Database(Environment^ env, DatabaseConfig^ config)
	:env_(env), config_(config),
	BdbComponent(env->logger_, String::Format("database (file name [{0}], database name [{1}])", env_->fileName_, config_->Name)) {
	DB* db;
	CheckApiOk(db_create(&db, env->dbEnv_, 0), "db_create");
	db_ = db;
	db_->set_errpfx(db_, "dbpfx");
	db_->set_errcall(db_, GetLogFunc());

	keysState_ = gcnew BufferState("keys, " + description_, config->KeyBufferConfig, logger_);
	valuesState_ = gcnew BufferState("values, " + description_, config->ValueBufferConfig, logger_);
}

void Database::LogErrorViaBdb(int error, String^ message) {
	CheckOpen();
	std::string stdMessage(msclr::interop::marshal_as<std::string>(message));
	db_->err(db_, error, stdMessage.c_str());
}

void Database::Open() {
	if (config_->EnableRecno)
		CheckApiOk(db_->set_flags(db_, DB_RECNUM), "db.set_flags");
	CheckApiOk(db_->set_priority(db_, static_cast<DB_CACHE_PRIORITY>(config_->CachePriority)), "db.set_priority");
	String^ localFileName = env_->fileName_;
	String^ localDatabaseName_ = config_->Name;
	std::string stdFileName(msclr::interop::marshal_as<std::string>(localFileName));
	std::string stdDatabaseName(msclr::interop::marshal_as<std::string>(localDatabaseName_));
	CheckApiOk(db_->open(db_, nullptr, stdFileName.c_str(), stdDatabaseName.c_str(), DB_BTREE, config_->IsReadonly ? DB_RDONLY : DB_CREATE, 0), "db.open");
}

void Database::Add(BytesSegment key, BytesSegment value) {
	CheckOpen();
	DBT_FOR_BYTES_SEGMENT(key, key);
	DBT_FOR_BYTES_SEGMENT(value, value);
	keysState_->CheckLength(keyLen);
	valuesState_->CheckLength(valueLen);
	CheckApiOk(db_->put(db_, nullptr, &keyDbt, &valueDbt, 0), "db.put");
}

void Database::Remove(BytesSegment key) {
	CheckOpen();
	DBT_FOR_BYTES_SEGMENT(key, key);
	int resultCode = db_->del(db_, nullptr, &keyDbt, 0);
	if (resultCode == DB_NOTFOUND)
		return;
	CheckApiOk(resultCode, "db.del");
}

DatabaseStatistics Database::GetStatistics(bool fast) {
	CheckOpen();
	if (fast)
		CheckRecordNumbersEnabled();
	DatabaseStatistics result;
	DB_BTREE_STAT* pDbStat;
	CheckApiOk(db_->stat(db_, nullptr, &pDbStat, fast ? DB_FAST_STAT : 0), "db.stat");
	*((DB_BTREE_STAT*)(&result)) = *pDbStat;
	free(pDbStat);
	return result;
}

void Database::CheckRecordNumbersEnabled() {
	if (!config_->EnableRecno)
		throw gcnew BdbException("Bdb was not configured to support record numbers, " + description_);
}

ICursor^ Database::Query(Range^ range, Direction direction, int skip, int take) {
	CheckOpen();
	if (skip > 0)
		CheckRecordNumbersEnabled();
	return gcnew SimpleCursor(this, range, direction == Direction::Ascending ? 1 : -1, skip, take);
}

BytesTable^ Database::Fetch([NotNull] array<Range^>^ ranges, Direction direction, int take, unsigned int keySuffixOffset, FetchOptions options) {
	CheckOpen();
	SuffixMergingFetcher fether(this, ranges, direction == Direction::Ascending ? 1 : -1, keySuffixOffset, options);
	return fether.Fetch(options, take);
}

BytesBuffer^ Database::Find(BytesSegment key) {
	CheckOpen();
	BufferAllocator^ valueAccessor = gcnew BufferAllocator(valuesState_, 1);
	int resultCode = DoFind(key, valueAccessor);
	if (resultCode == DB_BUFFER_SMALL) {
		valueAccessor->EnsureChunkCapacity(valueAccessor->buffer_->Length);
		resultCode = DoFind(key, valueAccessor);
	}
	if (resultCode == DB_NOTFOUND)
		return nullptr;
	CheckApiOk(resultCode, "db.get");
	return valueAccessor->buffer_;
}

int Database::DoFind(BytesSegment key, BufferAllocator^ valueAccessor) {
	DBT_FOR_BYTES_SEGMENT(key, key);
	DBT_FOR_BYTE_ACCESSOR(value, valueAccessor);
	int resultCode = db_->get(db_, nullptr, &keyDbt, &valueDbt, 0);
	valueAccessor->buffer_->Length = valueDbt.size;
	return resultCode;
}

void Database::Close() {
	if (TestingEnvironment::ThrowOnDatabaseClose != nullptr && TestingEnvironment::ThrowOnDatabaseClose->Count > 0)
		throw gcnew BdbException(TestingEnvironment::ThrowOnDatabaseClose->Dequeue());
	env_->CheckOpen();
	env_->UntrackDatabase(this);
	CheckApiOk(db_->close(db_, env_->config_->IsPersistent ? 0 : DB_NOSYNC), "db.close");
	db_ = nullptr;
}