﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using NUnit.Framework;
using System;
using System.IO;
using FASTER.core;
using FASTER.devices;
using System.Threading;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace FASTER.systest
{
    internal static class TestUtils
    {
        // Various categories used to group tests
        internal const string SmokeTestCategory = "Smoke";
        internal const string FasterKVTestCategory = "FasterKV";
        internal const string LockableUnsafeContextTestCategory = "LockableUnsafeContext";
        internal const string ReadCacheTestCategory = "ReadCache";
        internal const string LockTestCategory = "Locking";
        internal const string CheckpointRestoreCategory = "CheckpointRestore";
        internal const string RMW = "RMW";

        /// <summary>
        /// Delete a directory recursively
        /// </summary>
        /// <param name="path">The folder to delete</param>
        /// <param name="wait">If true, loop on exceptions that are retryable, and verify the directory no longer exists. Generally true on SetUp, false on TearDown</param>
        internal static void DeleteDirectory(string path, bool wait = false)
        {
            while (true)
            {
                try
                {
                    if (!Directory.Exists(path))
                        return;
                    foreach (string directory in Directory.GetDirectories(path))
                        DeleteDirectory(directory, wait);
                    break;
                }
                catch
                {
                }
            }

            bool retry = true;
            while (retry)
            {
                // Exceptions may happen due to a handle briefly remaining held after Dispose().
                retry = false;
                try
                {
                    Directory.Delete(path, true);
                }
                catch (Exception ex) when (ex is IOException ||
                                           ex is UnauthorizedAccessException)
                {
                    if (!wait)
                    {
                        try { Directory.Delete(path, true); }
                        catch { }
                        return;
                    }
                    retry = true;
                }
            }
            
            if (!wait)
                return;

            while (Directory.Exists(path))
                Thread.Yield();
        }

        /// <summary>
        /// Create a clean new directory, removing a previous one if needed.
        /// </summary>
        /// <param name="path"></param>
        internal static void RecreateDirectory(string path)
        {
            if (Directory.Exists(path))
                DeleteDirectory(path);

            // Don't catch; if this fails, so should the test
            Directory.CreateDirectory(path);
        }

        internal static bool IsRunningAzureTests => "yes".Equals(Environment.GetEnvironmentVariable("RunAzureTests")) || "yes".Equals(Environment.GetEnvironmentVariable("RUNAZURETESTS"));

        internal static void IgnoreIfNotRunningAzureTests()
        {
            // Need this environment variable set AND Azure Storage Emulator running
            if (!IsRunningAzureTests)
                Assert.Ignore("Environment variable RunAzureTests is not defined");
        }

        // Used to test the various devices by using the same test with VALUES parameter
        // Cannot use LocalStorageDevice from non-Windows OS platform
        public enum DeviceType
        {
#if WINDOWS
            LSD,
#endif
            EmulatedAzure,
            MLSD,
            LocalMemory
        }

        internal static IDevice CreateTestDevice(DeviceType testDeviceType, string filename, int latencyMs = 20, bool deleteOnClose = false)  // latencyMs works only for DeviceType = LocalMemory
        {
            IDevice device = null;
            bool preallocateFile = false;
            long capacity = -1; // Capacity unspecified
            bool recoverDevice = false;
            
            switch (testDeviceType)
            {
#if WINDOWS
                case DeviceType.LSD:
                    bool useIoCompletionPort = false;
                    bool disableFileBuffering = true;
#if NETSTANDARD || NET
                    if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))    // avoids CA1416 // Validate platform compatibility
#endif
                        device = new LocalStorageDevice(filename, preallocateFile, deleteOnClose, disableFileBuffering, capacity, recoverDevice, useIoCompletionPort);
                    break;
#endif
                case DeviceType.EmulatedAzure:
                    IgnoreIfNotRunningAzureTests();
                    device = new AzureStorageDevice(AzureEmulatedStorageString, AzureTestContainer, AzureTestDirectory, Path.GetFileName(filename), deleteOnClose: deleteOnClose);
                    break;
                case DeviceType.MLSD:
                    device = new ManagedLocalStorageDevice(filename, preallocateFile, deleteOnClose, capacity, recoverDevice);
                    break;
                // Emulated higher latency storage device - takes a disk latency arg (latencyMs) and emulates an IDevice using main memory, serving data at specified latency
                case DeviceType.LocalMemory:  
                    device = new LocalMemoryDevice(1L << 30, 1L << 30, 2, sector_size: 512, latencyMs: latencyMs, fileName: filename);  // 64 MB (1L << 26) is enough for our test cases
                    break;
            }

            return device;
        }

        private static string ConvertedClassName(bool forAzure = false)
        {
            // Make this all under one root folder named {prefix}, which is the base namespace name. All UT namespaces using this must start with this prefix.
            const string prefix = "FASTER.systest";
            Assert.IsTrue(TestContext.CurrentContext.Test.ClassName.StartsWith($"{prefix}."), $"Expected {prefix} prefix was not found");
            var suffix = TestContext.CurrentContext.Test.ClassName.Substring(prefix.Length + 1);
            return forAzure ? suffix : $"{prefix}/{suffix}";
        }

        internal static string MethodTestDir => @"D:\github\FASTER\cs\systest\log";
        // internal static string MethodTestDir => Path.Combine(TestContext.CurrentContext.TestDirectory, $"{ConvertedClassName()}_{TestContext.CurrentContext.Test.MethodName}");

        internal static string AzureTestContainer
        {
            get
            {
                var container = ConvertedClassName(forAzure: true).Replace('.', '-').ToLower();
                Microsoft.Azure.Storage.NameValidator.ValidateContainerName(container);
                return container;
            }
        }

        internal static string AzureTestDirectory => TestContext.CurrentContext.Test.MethodName;

        internal const string AzureEmulatedStorageString = "UseDevelopmentStorage=true;";

        internal enum AllocatorType
        {
            FixedBlittable,
            VarLenBlittable,
            Generic
        }

        internal enum SyncMode { Sync, Async };

        public enum ReadCopyDestination { Tail, ReadCache }

        public enum FlushMode { NoFlush, ReadOnly, OnDisk }

        public enum KeyEquality { Equal, NotEqual }

        internal static (Status status, TOutput output) GetSinglePendingResult<TKey, TValue, TInput, TOutput, TContext>(CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs)
            => GetSinglePendingResult(completedOutputs, out _);

        internal static (Status status, TOutput output) GetSinglePendingResult<TKey, TValue, TInput, TOutput, TContext>(CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs, out RecordMetadata recordMetadata)
        {
            Assert.IsTrue(completedOutputs.Next());
            var result = (completedOutputs.Current.Status, completedOutputs.Current.Output);
            recordMetadata = completedOutputs.Current.RecordMetadata;
            Assert.IsFalse(completedOutputs.Next());
            completedOutputs.Dispose();
            return result;
        }

        internal async static ValueTask DoTwoThreadRandomKeyTest(int count, Action<int> first, Action<int> second, Action<int> verification)
        {
            Task[] tasks = new Task[2];

            var rng = new Random(101);
            for (var iter = 0; iter < count; ++iter)
            {
                var arg = rng.Next(count);
                tasks[0] = Task.Factory.StartNew(() => first(arg));
                tasks[1] = Task.Factory.StartNew(() => second(arg));

                await Task.WhenAll(tasks);

                verification(arg);
            }
        }

        internal unsafe static bool FindKey<Key, Value>(this FasterKV<Key, Value> fht, ref Key key, out HashBucketEntry entry, out HashBucket* bucket, out int slot)
        {
            bucket = default;
            slot = default;
            entry = default;

            var hash = fht.Comparer.GetHashCode64(ref key);
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            return fht.FindTag(hash, tag, ref bucket, ref slot, ref entry);
        }

        internal static unsafe bool FindKey<Key, Value>(this FasterKV<Key, Value> fht, ref Key key, out HashBucketEntry entry) => FindKey(fht, ref key, out entry, out _, out _);
    }
}
