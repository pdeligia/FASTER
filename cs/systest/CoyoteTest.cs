// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using FASTER.core;
using FASTER.systest.LockableUnsafeContext;
using NUnit.Framework;
using System.Threading.Tasks;
using static FASTER.systest.TestUtils;
using System.Diagnostics;

namespace FASTER.systest.LockableUnsafeContext
{
    public static class CoyoteTest
    {
        [Microsoft.Coyote.SystematicTesting.Test]
        public static void RunCoyoteTest()
        {
            var test = new LockableUnsafeContextTests();
            test.Setup();
            test.LockNewRecordCompeteWithUpdateTest(LockOperationType.Lock, UpdateOp.Upsert);
            test.TearDown();
        }
    }
}
