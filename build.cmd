@echo off
set dotNetBasePath=%windir%\Microsoft.NET\Framework
if exist %dotNetBasePath%64 set dotNetBasePath=%dotNetBasePath%64
for /R %dotNetBasePath% %%i in (*msbuild.exe) do set msbuild=%%i
set target=_Src\SimpleBdb.sln
set vcTargetsPath=%ProgramFiles(x86)%\MSBuild\Microsoft.Cpp\v4.0\V110

call "%ProgramFiles(x86)%\Microsoft Visual Studio 11.0\VC\bin\x86_amd64\vcvarsx86_amd64.bat"
%msbuild% /t:Rebuild /v:m /p:Configuration=Release /p:Platform="x64" /p:VCTargetsPath="%vcTargetsPath%" %target% || exit /b 1