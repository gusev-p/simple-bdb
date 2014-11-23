#pragma once

using namespace JetBrains::Annotations;

namespace SimpleBdb {
	namespace Driver {

		//here, because typedefs can't be forward declared
		typedef unsigned char Byte;

		public enum class FetchOptions {
			Keys = 1,
			Values = 2,
			KeysAndValues = 3
		};

		//here, because can't inherit from forward declared class
		public interface class ICursor : public SimpleBdb::Utils::IForwardReader<SimpleBdb::Utils::BytesRecord^> {
			unsigned int GetTotalCount();
			SimpleBdb::Utils::BytesTable^ Fetch(FetchOptions options);
		};

		//here, because can't inherit from forward declared class
		namespace Implementation {
			public ref class BdbComponent abstract {
			public:
				~BdbComponent();
				!BdbComponent();
			internal:
				SimpleBdb::Utils::ILogger^ logger_;
				System::String^ description_;
				inline bool IsDisposed();
				virtual void CheckOpen();
			protected:
				BdbComponent([NotNull] SimpleBdb::Utils::ILogger^ logger, [NotNull] System::String^ description);
				virtual void Close() abstract;
				void CheckApiOk(int resultCode, System::String^ api);
				System::String^ FormatApiMessage(int resultCode, System::String^ api);

				typedef void(*BdbLogFunc)(const DB_ENV *, const char *, const char *);
				BdbLogFunc GetLogFunc();
			private:
				[System::Runtime::InteropServices::UnmanagedFunctionPointer(System::Runtime::InteropServices::CallingConvention::Cdecl)]
				delegate void LogErrorDelegate(const DB_ENV *, const char *prefix, const char *message);
				bool CanUseReferencedObjects();
				System::Delegate^ logErrorDelegate_;
				void LogError(const DB_ENV *, const char *prefix, const char *message);
				bool disposed_;
			};
		}
	}
}