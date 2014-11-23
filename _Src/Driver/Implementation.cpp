#include "Stdafx.h"
#include "Implementation.h"
#include "Interface.h"

using namespace SimpleBdb::Driver::Implementation;

using System::String;
using System::ObjectDisposedException;
using System::GC;
using System::Exception;
using System::Array;
using SimpleBdb::Utils::ILogger;
using SimpleBdb::Utils::Boundary;
using SimpleBdb::Utils::BytesBuffer;
using SimpleBdb::Driver::BytesBufferConfig;
using SimpleBdb::Driver::Direction;
using SimpleBdb::Driver::Byte;

BdbComponent::BdbComponent(ILogger^ logger, String^ description) :logger_(logger), description_(description), disposed_(false), 
	logErrorDelegate_(gcnew LogErrorDelegate(this, &BdbComponent::LogError)) {
}

bool BdbComponent::IsDisposed() {
	return disposed_;
}

void BdbComponent::CheckOpen() {
	if (disposed_)
		throw gcnew ObjectDisposedException(description_);
}

String^ BdbComponent::FormatApiMessage(int resultCode, String^ api) {
	return String::Format("api [{0}] failed, error code [{1}], {2}", api, resultCode, description_);
}

void BdbComponent::CheckApiOk(int resultCode, String^ api) {
	if (resultCode != 0)
		throw gcnew BdbApiException(FormatApiMessage(resultCode, api), resultCode);
}

BdbComponent::BdbLogFunc BdbComponent::GetLogFunc() {
	return  (BdbComponent::BdbLogFunc)System::Runtime::InteropServices::Marshal::GetFunctionPointerForDelegate(logErrorDelegate_).ToPointer();
}

bool BdbComponent::CanUseReferencedObjects() {
	bool isFinalizingForUnload = System::AppDomain::CurrentDomain->IsFinalizingForUnload();
	bool hasShutdownStarted = System::Environment::HasShutdownStarted;
	return !isFinalizingForUnload && !hasShutdownStarted;
}

void BdbComponent::LogError(const DB_ENV *, const char *prefix, const char *message) {
	if (CanUseReferencedObjects())
		logger_->Error("bdb reported error: " + gcnew String(prefix) + ", " + gcnew String(message) + ", " + description_);
}

BdbComponent::~BdbComponent() {
	if (disposed_)
		return;
	try {
		this->Close();
		disposed_ = true;
		GC::SuppressFinalize(this);
	}
	catch (Exception^ exception) {
		logger_->Error("dispose exception, " + description_, exception);
	}
}

BdbComponent::!BdbComponent() {
	if (disposed_)
		return;
	try {
		this->Close();
		disposed_ = true;
	}
	catch (Exception^ exception) {
		if (CanUseReferencedObjects())
			logger_->Error("finalizer exception, " + description_, exception);
	}
}

BufferState::BufferState(String^ description, BytesBufferConfig^ config, ILogger^ logger)
	:description_(description), config_(config), lengthInBytes_(config->Size), logger_(logger) {
	if (config_->Fixed && config_->Size < sizeof(unsigned int))
		throw gcnew BdbException(String::Format("fixed buffer size [{0}] can't be smaller than size of unsigned int [{1}], {2}",
		config_->Size, sizeof(unsigned int), description_));
}

void BufferState::UpdateLength(int newLengthInBytes) {
	CheckLength(newLengthInBytes);
	if (newLengthInBytes <= lengthInBytes_)
		return;
	logger_->Warn(String::Format("reallocated from [{0}] to [{1}], {2}", lengthInBytes_, newLengthInBytes, description_));
	lengthInBytes_ = newLengthInBytes;
}

void BufferState::CheckLength(int length) {
	if (config_->Fixed && length > config_->Size)
		throw gcnew BdbException(String::Format("requested length [{0}] is greater than size [{1}] of fixed buffer, {2}",
		length, config_->Size, description_));
}

int BufferState::GetLengthInBytes() {
	return lengthInBytes_;
}

BufferAllocator::BufferAllocator(BufferState^ state, unsigned int chunksCount) :buffer_(gcnew BytesBuffer()), state_(state), chunksCount_(chunksCount) {
	Allocate(state_->GetLengthInBytes());
}

void BufferAllocator::EnsureChunkCapacity(int capacity) {
	if (capacity <= chunkSize_)
		return;
	Allocate(capacity);
	state_->UpdateLength(capacity);
}

void BufferAllocator::Allocate(int capacity) {
	buffer_->DangerousBytes = gcnew array<Byte>(capacity * chunksCount_);
	chunkSize_ = capacity;
}