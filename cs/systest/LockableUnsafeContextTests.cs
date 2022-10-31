// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using FASTER.core;
using NUnit.Framework;
using System.Threading.Tasks;
using static FASTER.systest.TestUtils;
using System.Diagnostics;

namespace FASTER.systest.LockableUnsafeContext
{
    // Functions for the "Simple lock transaction" case, e.g.:
    //  - Lock key1, key2, key3, keyResult
    //  - Do some operation on value1, value2, value3 and write the result to valueResult
    internal class LockableUnsafeFunctions : SimpleFunctions<int, int>
    {
        internal long deletedRecordAddress;

        public override void PostSingleDeleter(ref int key, ref DeleteInfo deleteInfo)
        {
            deletedRecordAddress = deleteInfo.Address;
        }

        public override bool ConcurrentDeleter(ref int key, ref int value, ref DeleteInfo deleteInfo)
        {
            deletedRecordAddress = deleteInfo.Address;
            return true;
        }
    }

    internal class LockableUnsafeComparer : IFasterEqualityComparer<int>
    {
        internal int maxSleepMs;
        readonly Random rng = new(101);

        public bool Equals(ref int k1, ref int k2) => k1 == k2;

        public long GetHashCode64(ref int k)
        {
            // if (maxSleepMs > 0)
            //     Thread.Sleep(rng.Next(maxSleepMs));
            if (maxSleepMs > 0)
                Task.Delay(rng.Next(maxSleepMs)).Wait();
            return Utility.GetHashCode(k);
        }
    }

    public enum UpdateOp { Upsert, RMW, Delete }

    [TestFixture]
    class LockableUnsafeContextTests
    {
        const int numRecords = 1000;

        const int valueMult = 1_000_000;

        LockableUnsafeFunctions functions;
        LockableUnsafeComparer comparer;

        private FasterKV<int, int> fht;
        private ClientSession<int, int, int, int, Empty, LockableUnsafeFunctions> session;
        private IDevice log;

        [SetUp]
        public void Setup() => Setup(forRecovery: false);

        public void Setup(bool forRecovery)
        {
            if (!forRecovery)
            {
                DeleteDirectory(MethodTestDir, wait: true);
            }
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: false, recoverDevice: forRecovery);

            ReadCacheSettings readCacheSettings = default;
            CheckpointSettings checkpointSettings = default;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is ReadCopyDestination dest)
                {
                    if (dest == ReadCopyDestination.ReadCache)
                        readCacheSettings = new() { PageSizeBits = 12, MemorySizeBits = 22 };
                    break;
                }
                if (arg is CheckpointType chktType)
                {
                    checkpointSettings = new CheckpointSettings { CheckpointDir = MethodTestDir };
                    break;
                }
            }

            comparer = new LockableUnsafeComparer();
            functions = new LockableUnsafeFunctions();

            fht = new FasterKV<int, int>(1L << 20, new LogSettings { LogDevice = log, ObjectLogDevice = null, PageSizeBits = 12, MemorySizeBits = 22, ReadCacheSettings = readCacheSettings },
                                            checkpointSettings: checkpointSettings, comparer: comparer,
                                            disableLocking: false);
            session = fht.For(functions).NewSession<LockableUnsafeFunctions>();
        }

        [TearDown]
        public void TearDown() => TearDown(forRecovery: false);

        public void TearDown(bool forRecovery)
        {
            session?.Dispose();
            session = null;
            fht?.Dispose();
            fht = null;
            log?.Dispose();
            log = null;

            if (!forRecovery)
            {
                DeleteDirectory(MethodTestDir);
            }
        }

        void Populate()
        {
            for (int key = 0; key < numRecords; key++)
                Assert.IsFalse(session.Upsert(key, key * valueMult).IsPending);
        }

        static void ClearCountsOnError(ClientSession<int, int, int, int, Empty, LockableUnsafeFunctions> luContext)
        {
            // If we already have an exception, clear these counts so "Run" will not report them spuriously.
            luContext.sharedLockCount = 0;
            luContext.exclusiveLockCount = 0;
        }

        static void ClearCountsOnError(ClientSession<int, int, int, int, Empty, IFunctions<int, int, int, int, Empty>> luContext)
        {
            // If we already have an exception, clear these counts so "Run" will not report them spuriously.
            luContext.sharedLockCount = 0;
            luContext.exclusiveLockCount = 0;
        }

        static void ClearCountsOnError(ClientSession<long, long, long, long, Empty, IFunctions<long, long, long, long, Empty>> luContext)
        {
            // If we already have an exception, clear these counts so "Run" will not report them spuriously.
            luContext.sharedLockCount = 0;
            luContext.exclusiveLockCount = 0;
        }

        [Test]
        [Category(LockableUnsafeContextTestCategory)]
        public void LockNewRecordCompeteWithUpdateTest([Values(LockOperationType.Lock, LockOperationType.Unlock)] LockOperationType lockOp, [Values] UpdateOp updateOp)
        {
            const int numNewRecords = 100;

            using var updateSession = fht.NewSession(new SimpleFunctions<int, int>());
            using var lockSession = fht.NewSession(new SimpleFunctions<int, int>());

            var updateLuContext = updateSession.LockableUnsafeContext;
            var lockLuContext = lockSession.LockableUnsafeContext;

            LockType getLockType(int key) => ((key & 1) == 0) ? LockType.Exclusive : LockType.Shared;
            int getValue(int key) => key + valueMult;

            // If we are testing Delete, then we need to have the records ON-DISK first; Delete is a no-op for unfound records.
            if (updateOp == UpdateOp.Delete)
            {
                for (var key = numRecords; key < numRecords + numNewRecords; ++key)
                    Assert.IsFalse(session.Upsert(key, key * valueMult).IsPending);
                fht.Log.FlushAndEvict(wait: true);
            }

            // Now populate the main area of the log.
            Populate();

            lockLuContext.BeginUnsafe();
            lockLuContext.BeginLockable();

            HashSet<int> locks = new();
            void lockKey(int key)
            {
                lockLuContext.Lock(key, getLockType(key));
                locks.Add(key);
            }
            void unlockKey(int key)
            {
                lockLuContext.Unlock(key, getLockType(key));
                locks.Remove(key);
            }

            try
            {

                // If we are testing unlocking, then we need to lock first.
                if (lockOp == LockOperationType.Unlock)
                {
                    for (var key = numRecords; key < numRecords + numNewRecords; ++key)
                        lockKey(key);
                }

                // Sleep at varying durations for each call to comparer.GetHashCode, which is called at the start of Lock/Unlock and Upsert/RMW/Delete.
                comparer.maxSleepMs = 20;

                for (var key = numRecords; key < numRecords + numNewRecords; ++key)
                {
                    // Use Task instead of Thread because this propagates exceptions (such as Assert.* failures) back to this thread.
                    Task.WaitAll(Task.Run(() => locker(key)), Task.Run(() => updater(key)));
                    var (xlock, slockCount) = lockLuContext.IsLocked(key);
                    var expectedXlock = getLockType(key) == LockType.Exclusive && lockOp != LockOperationType.Unlock;
                    var expectedSlock = getLockType(key) == LockType.Shared && lockOp != LockOperationType.Unlock;
                    Assert.AreEqual(expectedXlock, xlock);
                    Assert.AreEqual(expectedSlock, slockCount > 0);

                    if (lockOp == LockOperationType.Lock)
                    {
                        // There should be no entries in the locktable now; they should all be on the RecordInfo.
                        Assert.IsFalse(fht.LockTable.IsActive, $"count = {fht.LockTable.dict.Count}");
                    }
                    else
                    {
                        // We are unlocking so should remove one record for each iteration.
                        Assert.AreEqual(numNewRecords + numRecords - key - 1, fht.LockTable.dict.Count);
                    }
                }

                // Unlock all the keys we are expecting to unlock, which ensures all the locks were applied to RecordInfos as expected.
                foreach (var key in locks.ToArray())
                    unlockKey(key);
            }
            catch (Exception)
            {
                ClearCountsOnError(lockSession);
                throw;
            }
            finally
            {
                lockLuContext.EndLockable();
                lockLuContext.EndUnsafe();
            }

            void locker(int key)
            {
                try
                {
                    // Begin/EndLockable are called outside this function; we could not EndLockable in here as the lock lifetime is beyond that.
                    // (BeginLockable's scope is the session; BeginUnsafe's scope is the thread. The session is still "mono-threaded" here because
                    // only one thread at a time is making calls on it.)
                    lockLuContext.BeginUnsafe();
                    if (lockOp == LockOperationType.Lock)
                        lockKey(key);
                    else
                        unlockKey(key);
                }
                catch (Exception)
                {
                    ClearCountsOnError(lockSession);
                    throw;
                }
                finally
                {
                    lockLuContext.EndUnsafe();
                }
            }

            void updater(int key)
            {
                updateLuContext.BeginUnsafe();

                try
                {
                    // Use the LuContext here even though we're not doing locking, because we don't want the ephemeral locks to be tried for this test
                    // (the test would hang when trying to acquire the ephemeral lock).
                    var status = updateOp switch
                    {
                        UpdateOp.Upsert => updateLuContext.Upsert(key, getValue(key)),
                        UpdateOp.RMW => updateLuContext.RMW(key, getValue(key)),
                        UpdateOp.Delete => updateLuContext.Delete(key),
                        _ => new(StatusCode.Error)
                    };
                    Assert.IsFalse(status.IsFaulted, $"Unexpected UpdateOp {updateOp}, status {status}");
                    Assert.IsFalse(status.Found, status.ToString());
                    Assert.IsTrue(status.Record.Created, status.ToString());
                }
                catch (Exception)
                {
                    ClearCountsOnError(updateSession);
                    throw;
                }
                finally
                {
                    updateLuContext.EndUnsafe();
                }
            }
        }
    }
}
