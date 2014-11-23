simple-bdb
==========

Simple and fast C# driver for BerkleyDBs

What's wrong with official Oracle .NET driver ?
===============================================

Performance. Oracle C# driver is generated from C code
using http://www.swig.org/ tool. While pretty good in
general swig incurs too much overhead for bdb interaction.
Any driver api invokation results in large number of pinvoke's,
which are costly. Other source of overhead is copying of byte
arrays between unmanaged and managed memory.

Features
========
* Written in C++/CLI thereby reducing overhead of each individual pinvoke.

* Fetch api allows to retrieve chunk of data from unmanaged memory in one call
futher reducing the number of pinvokes.

* Range-union fetch allows to query by first part of composite key
with ordering by second part.

* Cursor api can be used to iterate over large data sets with no
redundant byte array copying.

Api
===

Keywords
=========
Oracle, bdb, BerkleyDb, .NET, C#, BerkleyDb Driver
