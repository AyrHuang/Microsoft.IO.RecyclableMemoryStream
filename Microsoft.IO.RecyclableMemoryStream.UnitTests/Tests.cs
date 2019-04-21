// ---------------------------------------------------------------------
// Copyright (c) 2015 Microsoft
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
// ---------------------------------------------------------------------
namespace Microsoft.IO.RecyclableMemoryStream.UnitTests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading.Tasks;

    using Microsoft.IO;
    using NUnit.Framework;

    /// <summary>
    /// Full test suite. It is abstract to allow parameters of the memory manager to be modified and tested in different
    /// combinations.
    /// </summary>
    public abstract class BaseRecyclableMemoryStreamTests
    {
        protected const int DefaultBlockSize = 16384;
        protected const int DefaultLargeBufferMultiple = 1 << 20;
        protected const int DefaultMaximumBufferSize = 8 * (1 << 20);
        protected const string DefaultTag = "NUnit";
        private const int MemoryStreamDisposed = 2;
        private const int MemoryStreamDoubleDispose = 3;

        private readonly Random random = new Random();

        #region RecyclableMemoryManager Tests
        [Test]
        public virtual void RecyclableMemoryManagerUsingMultipleOrExponentialLargeBuffer()
        {
            var recyclableMemoryStreamManager = GetMemoryManager();
            Assert.That(
                recyclableMemoryStreamManager.UseMultipleLargeBuffer,
                Is.True
            );
            Assert.That(
                recyclableMemoryStreamManager.UseExponentialLargeBuffer,
                Is.False
            );
        }

        [Test]
        public void RecyclableMemoryManagerThrowsExceptionOnZeroBlockSize()
        {
            // Failure case
            Assert.Throws<ArgumentOutOfRangeException>(
                () => new RecyclableMemoryStreamManager(0, 100, 200, useExponentialLargeBuffer)
            );
            Assert.Throws<ArgumentOutOfRangeException>(
                () => new RecyclableMemoryStreamManager(-1, 100, 200, useExponentialLargeBuffer)
            );

            // Success case
            Assert.DoesNotThrow(
                () => new RecyclableMemoryStreamManager(1, 100, 200, useExponentialLargeBuffer)
            );
        }

        [Test]
        public void RecyclableMemoryManagerThrowsExceptionOnZeroLargeBufferMultipleSize()
        {
            // Failure case
            Assert.Throws<ArgumentOutOfRangeException>(
                () => new RecyclableMemoryStreamManager(100, 0, 200, useExponentialLargeBuffer)
            );
            Assert.Throws<ArgumentOutOfRangeException>(
                () => new RecyclableMemoryStreamManager(100, -1, 200, useExponentialLargeBuffer)
            );

            // Success case
            Assert.DoesNotThrow(
                () => new RecyclableMemoryStreamManager(100, 100, 200, useExponentialLargeBuffer)
            );
        }

        [Test]
        public void RecyclableMemoryManagerThrowsExceptionOnMaximumBufferSizeLessThanBlockSize()
        {
            // Failure case
            Assert.Throws<ArgumentOutOfRangeException>(
                () => new RecyclableMemoryStreamManager(100, 100, 99, useExponentialLargeBuffer)
            );

            // Success case
            Assert.DoesNotThrow(
                () => new RecyclableMemoryStreamManager(100, 100, 100, useExponentialLargeBuffer)
            );
        }

        [Test]
        public virtual void RecyclableMemoryManagerThrowsExceptionOnMaximumBufferNotMultipleOrExponentialOfLargeBufferMultiple()
        {
            // Failure case
            Assert.Throws<ArgumentException>(
                () => new RecyclableMemoryStreamManager(100, 1024, 2025, useExponentialLargeBuffer)
            );
            Assert.Throws<ArgumentException>(
                () => new RecyclableMemoryStreamManager(100, 1024, 2023, useExponentialLargeBuffer)
            );

            // Success case
            Assert.DoesNotThrow(
                () => new RecyclableMemoryStreamManager(100, 1024, 2048, useExponentialLargeBuffer)
            );
        }

        [Test]
        public virtual void GetLargeBufferAlwaysAMultipleOrExponentialOfMegabyteAndAtLeastAsMuchAsRequestedForLargeBuffer()
        {
            const int step = 200000;
            const int start = 1;
            const int end = 16000000;

            var recyclableMemoryStreamManager = GetMemoryManager();
            for (int requiredSize = start; requiredSize <= end; requiredSize += step)
            {
                var buffer = recyclableMemoryStreamManager.GetLargeBuffer(
                    requiredSize,
                    DefaultTag
                );
                Assert.That(
                    buffer.Length >= requiredSize,
                    Is.True
                );
                Assert.That(
                    (buffer.Length % recyclableMemoryStreamManager.LargeBufferMultiple) == 0, Is.True,
                    $"buffer length of {buffer.Length} is not a multiple of {recyclableMemoryStreamManager.LargeBufferMultiple}"
                );
            }
        }

        [Test]
        public virtual void AllMultiplesOrExponentialUpToMaxCanBePooled()
        {
            const int BlockSize = 100;
            const int LargeBufferMultiple = 1000;
            const int MaxBufferSize = 8000;

            for (var size = LargeBufferMultiple; size <= MaxBufferSize; size += LargeBufferMultiple)
            {
                var recyclableMemoryStreamManager = new RecyclableMemoryStreamManager(
                    BlockSize,
                    LargeBufferMultiple,
                    MaxBufferSize, 
                    useExponentialLargeBuffer
                )
                {
                    AggressiveBufferReturn = AggressiveBufferRelease
                };
                var buffer = recyclableMemoryStreamManager.GetLargeBuffer(
                    size,
                    DefaultTag
                );
                Assert.That(
                    recyclableMemoryStreamManager.LargePoolFreeSize,
                    Is.EqualTo(0)
                );
                Assert.That(
                    recyclableMemoryStreamManager.LargePoolInUseSize,
                    Is.EqualTo(size)
                );

                recyclableMemoryStreamManager.ReturnLargeBuffer(
                    buffer,
                    DefaultTag
                );
                Assert.That(
                    recyclableMemoryStreamManager.LargePoolFreeSize,
                    Is.EqualTo(size)
                );
                Assert.That(
                    recyclableMemoryStreamManager.LargePoolInUseSize,
                    Is.EqualTo(0)
                );
            }
        }

        /*
         * TODO: clocke to release logging libraries to enable some tests.
        [Test]
        public void GetVeryLargeBufferRecordsCallStack()
        {
            var logger = LogManager.CreateMemoryLogger();
            logger.SubscribeToEvents(Events.Writer, EventLevel.Verbose);

            var memMgr = GetMemoryManager();
            memMgr.GenerateCallStacks = true;
            var buffer = memMgr.GetLargeBuffer(memMgr.MaximumBufferSize + 1, DefaultTag);
            // wait for log to flush
            GC.Collect(1);
            GC.WaitForPendingFinalizers();
            Thread.Sleep(250);

            var log = Encoding.UTF8.GetString(logger.Stream.GetBuffer(), 0, (int)logger.Stream.Length);
            Assert.That(log, Is.StringContaining("MemoryStreamNonPooledLargeBufferCreated"));
            Assert.That(log, Is.StringContaining("GetLargeBuffer"));
            Assert.That(log, Is.StringContaining(buffer.Length.ToString()));
        }
        */

        [Test]
        public void ReturnLargerBufferWithNullBufferThrowsException()
        {
            var recyclableMemoryStreamManager = GetMemoryManager();
            Assert.Throws<ArgumentNullException>(
                () => recyclableMemoryStreamManager.ReturnLargeBuffer(null, DefaultTag)
            );
        }

        [Test]
        public void ReturnLargeBufferWithWrongSizedBufferThrowsException()
        {
            var recyclableMemoryStreamManager = GetMemoryManager();
            var buffer = new byte[100];
            Assert.Throws<ArgumentException>(
                () => recyclableMemoryStreamManager.ReturnLargeBuffer(buffer, DefaultTag)
            );
        }

        [Test]
        public void ReturnNullBlockThrowsException()
        {
            var recyclableMemoryStreamManager = GetMemoryManager();
            Assert.Throws<ArgumentNullException>(
                () => recyclableMemoryStreamManager.ReturnBlocks(null, string.Empty)
            );
        }

        [Test]
        public void ReturnBlocksWithInvalidBuffersThrowsException()
        {
            var buffers = new byte[3][];
            var recyclableMemoryStreamManager = GetMemoryManager();
            buffers[0] = recyclableMemoryStreamManager.GetBlock();
            buffers[1] = new byte[recyclableMemoryStreamManager.BlockSize + 1];
            buffers[2] = recyclableMemoryStreamManager.GetBlock();
            Assert.Throws<ArgumentException>(
                () => recyclableMemoryStreamManager.ReturnBlocks(buffers, string.Empty)
            );
        }

        [Test]
        public virtual void RequestTooLargeBufferAdjustsInUseCounter()
        {
            var recyclableMemoryStreamManager = GetMemoryManager();
            var buffer = recyclableMemoryStreamManager.GetLargeBuffer(
                recyclableMemoryStreamManager.MaximumBufferSize + 1,
                DefaultTag
            );

            int bufferSize = recyclableMemoryStreamManager.MaximumBufferSize + recyclableMemoryStreamManager.LargeBufferMultiple;
            Assert.That(
                buffer.Length,
                Is.EqualTo(bufferSize)
            );
            Assert.That(
                recyclableMemoryStreamManager.LargePoolInUseSize,
                Is.EqualTo(buffer.Length)
            );
        }

        [Test]
        public void ReturnTooLargeBufferDoesNotReturnToPool()
        {
            var recyclableMemoryStreamManager = GetMemoryManager();
            var buffer = recyclableMemoryStreamManager.GetLargeBuffer(
                recyclableMemoryStreamManager.MaximumBufferSize + 1,
                DefaultTag
            );

            recyclableMemoryStreamManager.ReturnLargeBuffer(
                buffer,
                DefaultTag
            );
            Assert.That(
                recyclableMemoryStreamManager.LargePoolInUseSize,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.LargePoolFreeSize,
                Is.EqualTo(0)
            );
        }

        [Test]
        public void ReturnZeroLengthBufferThrowsException()
        {
            var recyclableMemoryStreamManager = GetMemoryManager();
            var emptyBuffer = new byte[0];
            Assert.Throws<ArgumentException>(
                () => recyclableMemoryStreamManager.ReturnLargeBuffer(emptyBuffer, DefaultTag)
            );
        }

        [Test]
        public void ReturningBlocksAreDroppedIfEnoughFree()
        {
            const int MaxFreeBuffersAllowed = 2;
            const int BuffersToTest = MaxFreeBuffersAllowed + 1;

            // Only allow 2 blocks in the free pool at a time
            var recyclableMemoryStreamManager = GetMemoryManager();
            recyclableMemoryStreamManager.MaximumFreeSmallPoolBytes = MaxFreeBuffersAllowed * recyclableMemoryStreamManager.BlockSize;
            var buffers = new byte[BuffersToTest][];
            for (var i = 0; i < buffers.Length; ++i)
            {
                buffers[i] = recyclableMemoryStreamManager.GetBlock();
            }

            Assert.That(
                recyclableMemoryStreamManager.SmallPoolFreeSize,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolInUseSize,
                Is.EqualTo(BuffersToTest * recyclableMemoryStreamManager.BlockSize)
            );

            // All but one buffer should be returned to pool
            recyclableMemoryStreamManager.ReturnBlocks(
                buffers,
                string.Empty
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolFreeSize,
                Is.EqualTo(recyclableMemoryStreamManager.MaximumFreeSmallPoolBytes)
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolInUseSize,
                Is.EqualTo(0)
            );
        }

        [Test]
        public void ReturningBlocksNeverDroppedIfMaxFreeSizeZero()
        {
            const int BuffersToTest = 99;

            var recyclableMemoryStreamManager = GetMemoryManager();
            recyclableMemoryStreamManager.MaximumFreeSmallPoolBytes = 0;
            var buffers = new byte[BuffersToTest][];
            for (var i = 0; i < buffers.Length; ++i)
            {
                buffers[i] = recyclableMemoryStreamManager.GetBlock();
            }

            Assert.That(
                recyclableMemoryStreamManager.SmallPoolFreeSize,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolInUseSize,
                Is.EqualTo(BuffersToTest * recyclableMemoryStreamManager.BlockSize)
            );

            recyclableMemoryStreamManager.ReturnBlocks(
                buffers,
                string.Empty
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolFreeSize,
                Is.EqualTo(BuffersToTest * recyclableMemoryStreamManager.BlockSize)
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolInUseSize,
                Is.EqualTo(0)
            );
        }

        [Test]
        public void ReturningLargeBufferIsDroppedIfEnoughFree()
        {
            TestDroppingLargeBuffer(8000);
        }

        [Test]
        public void ReturningLargeBufferNeverDroppedIfMaxFreeSizeZero()
        {
            TestDroppingLargeBuffer(0);
        }

        protected virtual void TestDroppingLargeBuffer(long maxFreeLargeBufferSize)
        {
            const int BlockSize = 100;
            const int LargeBufferMultiple = 1000;
            const int MaxBufferSize = 8000;

            for (var size = LargeBufferMultiple; size <= MaxBufferSize; size += LargeBufferMultiple)
            {
                var recyclableMemoryStreamManager = new RecyclableMemoryStreamManager(
                    BlockSize,
                    LargeBufferMultiple,
                    MaxBufferSize,
                    useExponentialLargeBuffer
                )
                {
                    AggressiveBufferReturn = AggressiveBufferRelease,
                    MaximumFreeLargePoolBytes = maxFreeLargeBufferSize
                };

                var buffers = new List<byte[]>();

                //Get one extra buffer
                var buffersToRetrieve = (maxFreeLargeBufferSize > 0)
                                        ? (maxFreeLargeBufferSize / size + 1)
                                        : 10;
                for (var i = 0; i < buffersToRetrieve; i++)
                {
                    var buffer = recyclableMemoryStreamManager.GetLargeBuffer(
                        size,
                        DefaultTag
                    );
                    buffers.Add(
                        buffer
                    );
                }
                Assert.That(
                    recyclableMemoryStreamManager.LargePoolInUseSize,
                    Is.EqualTo(size * buffersToRetrieve)
                );
                Assert.That(
                    recyclableMemoryStreamManager.LargePoolFreeSize,
                    Is.EqualTo(0)
                );
                foreach (var buffer in buffers)
                {
                    recyclableMemoryStreamManager.ReturnLargeBuffer(
                        buffer,
                        DefaultTag
                    );
                }
                Assert.That(
                    recyclableMemoryStreamManager.LargePoolInUseSize,
                    Is.EqualTo(0)
                );
                if (maxFreeLargeBufferSize > 0)
                {
                    Assert.That(
                        recyclableMemoryStreamManager.LargePoolFreeSize,
                        Is.LessThanOrEqualTo(maxFreeLargeBufferSize)
                    );
                }
                else
                {
                    Assert.That(
                        recyclableMemoryStreamManager.LargePoolFreeSize,
                        Is.EqualTo(buffersToRetrieve * size)
                    );
                }
            }
        }

        [Test]
        public void GettingBlockAdjustsFreeAndInUseSize()
        {
            var recyclableMemoryStreamManager = GetMemoryManager();
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolFreeSize,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolInUseSize,
                Is.EqualTo(0)
            );

            // This should create a new block
            var blockCreate = recyclableMemoryStreamManager.GetBlock();
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolFreeSize,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolInUseSize,
                Is.EqualTo(recyclableMemoryStreamManager.BlockSize)
            );

            recyclableMemoryStreamManager.ReturnBlocks(
                new List<byte[]> {blockCreate},
                string.Empty
            );

            Assert.That(
                recyclableMemoryStreamManager.SmallPoolFreeSize,
                Is.EqualTo(recyclableMemoryStreamManager.BlockSize)
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolInUseSize,
                Is.EqualTo(0)
            );

            // This should get an existing block
            var blockExist = recyclableMemoryStreamManager.GetBlock();
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolFreeSize,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolInUseSize,
                Is.EqualTo(recyclableMemoryStreamManager.BlockSize)
            );

            recyclableMemoryStreamManager.ReturnBlocks(
                new List<byte[]> {blockExist},
                string.Empty
            );

            Assert.That(
                recyclableMemoryStreamManager.SmallPoolFreeSize,
                Is.EqualTo(recyclableMemoryStreamManager.BlockSize)
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolInUseSize,
                Is.EqualTo(0)
            );
        }
        #endregion

        #region GetBuffer Tests
        [Test]
        public void GetBufferReturnsSingleBlockForBlockSize()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var size = recyclableMemoryStream.MemoryManager.BlockSize;
            var buffer = GetRandomBuffer(size);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );
            var returnedBuffer = recyclableMemoryStream.GetBuffer();
            Assert.That(
                returnedBuffer.Length,
                Is.EqualTo(recyclableMemoryStream.MemoryManager.BlockSize)
            );
        }

        [Test]
        public void GetBufferReturnsSingleBlockForLessThanBlockSize()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var size = recyclableMemoryStream.MemoryManager.BlockSize - 1;
            var buffer = GetRandomBuffer(size);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );
            var returnedBuffer = recyclableMemoryStream.GetBuffer();
            Assert.That(
                returnedBuffer.Length,
                Is.EqualTo(recyclableMemoryStream.MemoryManager.BlockSize)
            );
        }

        [Test]
        public void GetBufferReturnsLargeBufferForMoreThanBlockSize()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var size = recyclableMemoryStream.MemoryManager.BlockSize + 1;
            var buffer = GetRandomBuffer(size);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );
            var returnedBuffer = recyclableMemoryStream.GetBuffer();
            Assert.That(
                returnedBuffer.Length,
                Is.EqualTo(recyclableMemoryStream.MemoryManager.LargeBufferMultiple)
            );
        }

        [Test]
        public void GetBufferReturnsSameLarge()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var buffer = GetRandomBuffer(recyclableMemoryStream.MemoryManager.LargeBufferMultiple);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );
            var returnedBuffer = recyclableMemoryStream.GetBuffer();
            var returnedBuffer2 = recyclableMemoryStream.GetBuffer();
            Assert.That(
                returnedBuffer,
                Is.SameAs(returnedBuffer2)
            );
        }

        [Test]
        public void GetBufferAdjustsLargePoolFreeSize()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var recyclableMemoryStreamManager = recyclableMemoryStream.MemoryManager;
            var bufferLength = recyclableMemoryStream.MemoryManager.BlockSize * 4;
            var buffer = GetRandomBuffer(bufferLength);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );

            var newBufferCreate = recyclableMemoryStream.GetBuffer();

            recyclableMemoryStream.Dispose();

            Assert.That(
                recyclableMemoryStreamManager.LargePoolFreeSize,
                Is.EqualTo(newBufferCreate.Length)
            );

            var recyclableMemoryStreamRecreate = new RecyclableMemoryStream(recyclableMemoryStreamManager);
            recyclableMemoryStreamRecreate.Write(buffer, 0, buffer.Length);

            var newBufferRecreate = recyclableMemoryStreamRecreate.GetBuffer();
            Assert.That(
                newBufferRecreate.Length,
                Is.EqualTo(newBufferCreate.Length)
            );
            Assert.That(
                recyclableMemoryStreamManager.LargePoolFreeSize,
                Is.EqualTo(0)
            );
        }

        [Test]
        public void CallingWriteAfterLargeGetBufferDoesNotLoseData()
        {
            var recyclableMemoryStream = GetDefaultStream();
            recyclableMemoryStream.Capacity = recyclableMemoryStream.MemoryManager.BlockSize + 1;
            var bufferCreate = recyclableMemoryStream.GetBuffer();
            bufferCreate[recyclableMemoryStream.MemoryManager.BlockSize] = 13;

            recyclableMemoryStream.Position = recyclableMemoryStream.MemoryManager.BlockSize + 1;
            var bytesToWrite = GetRandomBuffer(10);
            recyclableMemoryStream.Write(
                bytesToWrite,
                0,
                bytesToWrite.Length
            );

            var bufferExist = recyclableMemoryStream.GetBuffer();

            Assert.That(
                bufferExist[recyclableMemoryStream.MemoryManager.BlockSize],
                Is.EqualTo(13)
            );
            RMSAssert.BuffersAreEqual(
                bufferExist,
                recyclableMemoryStream.MemoryManager.BlockSize + 1,
                bytesToWrite,
                0,
                bytesToWrite.Length
            );
            Assert.That(
                recyclableMemoryStream.Position,
                Is.EqualTo(recyclableMemoryStream.MemoryManager.BlockSize + 1 + bytesToWrite.Length)
            );
        }

        [Test]
        public void CallingWriteByteAfterLargeGetBufferDoesNotLoseData()
        {
            var recyclableMemoryStream = GetDefaultStream();
            recyclableMemoryStream.Capacity = recyclableMemoryStream.MemoryManager.BlockSize + 1;
            var buffer = recyclableMemoryStream.GetBuffer();
            buffer[recyclableMemoryStream.MemoryManager.BlockSize] = 13;

            recyclableMemoryStream.Position = recyclableMemoryStream.MemoryManager.BlockSize + 1;
            recyclableMemoryStream.WriteByte(14);

            buffer = recyclableMemoryStream.GetBuffer();

            Assert.That(
                buffer[recyclableMemoryStream.MemoryManager.BlockSize],
                Is.EqualTo(13)
            );
            Assert.That(
                buffer[recyclableMemoryStream.MemoryManager.BlockSize + 1],
                Is.EqualTo(14)
            );
            Assert.That(
                recyclableMemoryStream.Position,
                Is.EqualTo(recyclableMemoryStream.MemoryManager.BlockSize + 2)
            );
        }

        [Test]
        public void GiantAllocationSucceeds()
        {
            var recyclableMemoryStreamManager = new RecyclableMemoryStreamManager();

            MemoryStream memoryStream = null;
            for (var i = -1; i < 2; ++i)
            {
                int requestedSize = int.MaxValue - (recyclableMemoryStreamManager.BlockSize + i);
                memoryStream = recyclableMemoryStreamManager.GetStream(
                    null,
                    requestedSize
                );
                Assert.IsTrue(
                    memoryStream.Capacity >= requestedSize
                );
            }

            memoryStream = recyclableMemoryStreamManager.GetStream(
                null,
                int.MaxValue
            );
            Assert.IsTrue(memoryStream.Capacity == int.MaxValue);
        }
        #endregion

        #region Constructor tests
        [Test]
        public void StreamHasTagAndGuid()
        {
            const string expectedTag = "Nunit Test";

            var recyclableMemoryStream = new RecyclableMemoryStream(
                GetMemoryManager(),
                expectedTag
            );
            Assert.That(
                recyclableMemoryStream.Id,
                Is.Not.EqualTo(Guid.Empty)
            );
            Assert.That(
                recyclableMemoryStream.Tag,
                Is.EqualTo(expectedTag)
            );
        }

        [Test]
        public void StreamHasDefaultCapacity()
        {
            var recyclableMemoryStreamManager = GetMemoryManager();
            var recyclableMemoryStream = new RecyclableMemoryStream(recyclableMemoryStreamManager);
            Assert.That(
                recyclableMemoryStream.Capacity,
                Is.EqualTo(recyclableMemoryStreamManager.BlockSize)
            );
        }

        [Test]
        public void ActualCapacityAtLeastRequestedCapacityAndMultipleOfBlockSize()
        {
            var recyclableMemoryStreamManager = GetMemoryManager();
            var requestedSize = recyclableMemoryStreamManager.BlockSize + 1;
            var recyclableMemoryStream = new RecyclableMemoryStream(
                recyclableMemoryStreamManager,
                string.Empty,
                requestedSize
            );
            Assert.That(
                recyclableMemoryStream.Capacity,
                Is.GreaterThanOrEqualTo(requestedSize)
            );
            Assert.That(
                0 == (recyclableMemoryStream.Capacity % recyclableMemoryStreamManager.BlockSize),
                Is.True,
                "stream capacity is not a multiple of the block size"
            );
        }

        [Test]
        public void AllocationStackIsRecorded()
        {
            var recyclableMemoryStreamManager = GetMemoryManager();
            recyclableMemoryStreamManager.GenerateCallStacks = true;

            var recyclableMemoryStream = new RecyclableMemoryStream(recyclableMemoryStreamManager);
            Assert.That(
                recyclableMemoryStream.AllocationStack,
                Does.Contain("RecyclableMemoryStream..ctor")
            );
            recyclableMemoryStream.Dispose();

            recyclableMemoryStreamManager.GenerateCallStacks = false;

            var recyclableMemoryStreamRecreate = new RecyclableMemoryStream(recyclableMemoryStreamManager);
            Assert.That(
                recyclableMemoryStreamRecreate.AllocationStack,
                Is.Null
            );
            recyclableMemoryStreamRecreate.Dispose();
        }
        #endregion

        #region Write Tests
        [Test]
        public void WriteUpdatesLengthAndPosition()
        {
            const int expectedLength = 100;

            var recyclableMemoryStreamManager = GetMemoryManager();
            var recyclableMemoryStream = new RecyclableMemoryStream(
                recyclableMemoryStreamManager,
                string.Empty,
                expectedLength
            );
            var buffer = GetRandomBuffer(expectedLength);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );
            Assert.That(
                recyclableMemoryStream.Length,
                Is.EqualTo(expectedLength)
            );
            Assert.That(
                recyclableMemoryStream.Position,
                Is.EqualTo(expectedLength)
            );
        }

        [Test]
        public void WriteInMiddleOfBufferDoesNotChangeLength()
        {
            const int expectedLength = 100;
            const int smallBufferLength = 25;

            var recyclableMemoryStream = GetDefaultStream();
            var buffer = GetRandomBuffer(expectedLength);
            recyclableMemoryStream.Write(
                buffer,
                0,
                expectedLength
            );
            Assert.That(
                recyclableMemoryStream.Length,
                Is.EqualTo(expectedLength)
            );

            var smallBuffer = GetRandomBuffer(smallBufferLength);
            recyclableMemoryStream.Position = 0;
            recyclableMemoryStream.Write(
                smallBuffer,
                0,
                smallBufferLength
            );
            Assert.That(
                recyclableMemoryStream.Length,
                Is.EqualTo(expectedLength)
            );
        }

        [Test]
        public void WriteSmallBufferStoresDataCorrectly()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var buffer = GetRandomBuffer(100);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );
            RMSAssert.BuffersAreEqual(
                buffer,
                recyclableMemoryStream.GetBuffer(),
                buffer.Length
            );
        }

        [Test]
        public void WriteLargeBufferStoresDataCorrectly()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var buffer = GetRandomBuffer(recyclableMemoryStream.MemoryManager.BlockSize + 1);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );
            RMSAssert.BuffersAreEqual(
                buffer,
                recyclableMemoryStream.GetBuffer(),
                buffer.Length
            );
        }

        [Test]
        public void WritePastEndIncreasesCapacity()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var buffer = GetRandomBuffer(DefaultBlockSize);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );
            Assert.That(
                recyclableMemoryStream.Capacity,
                Is.EqualTo(DefaultBlockSize)
            );
            Assert.That(
                recyclableMemoryStream.MemoryManager.SmallPoolInUseSize,
                Is.EqualTo(DefaultBlockSize)
            );
            recyclableMemoryStream.Write(
                new byte[] {0},
                0,
                1
            );
            Assert.That(
                recyclableMemoryStream.Capacity,
                Is.EqualTo(2 * DefaultBlockSize)
            );
            Assert.That(
                recyclableMemoryStream.MemoryManager.SmallPoolInUseSize,
                Is.EqualTo(2 * DefaultBlockSize)
            );
        }

        [Test]
        public void WritePastEndOfLargeBufferIncreasesCapacityAndCopiesBuffer()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var buffer = GetRandomBuffer(recyclableMemoryStream.MemoryManager.LargeBufferMultiple);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );

            var get1 = recyclableMemoryStream.GetBuffer();
            Assert.That(
                get1.Length,
                Is.EqualTo(recyclableMemoryStream.MemoryManager.LargeBufferMultiple)
            );
            recyclableMemoryStream.Write(
                buffer,
                0,
                1
            );

            var get2 = recyclableMemoryStream.GetBuffer();
            Assert.That(
                recyclableMemoryStream.Length,
                Is.EqualTo(recyclableMemoryStream.MemoryManager.LargeBufferMultiple + 1)
            );
            Assert.That(
                get2.Length,
                Is.EqualTo(recyclableMemoryStream.MemoryManager.LargeBufferMultiple * 2)
            );
            RMSAssert.BuffersAreEqual(
                get1,
                get2,
                (int)recyclableMemoryStream.Length - 1
            );
            Assert.That(
                get2[recyclableMemoryStream.MemoryManager.LargeBufferMultiple],
                Is.EqualTo(buffer[0])
            );
        }

        [Test]
        public void WriteAfterLargeBufferDoesNotAllocateMoreBlocks()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var buffer = GetRandomBuffer(recyclableMemoryStream.MemoryManager.BlockSize + 1);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );

            var inUseBlockBytes = recyclableMemoryStream.MemoryManager.SmallPoolInUseSize;
            recyclableMemoryStream.GetBuffer();
            Assert.That(
                recyclableMemoryStream.MemoryManager.SmallPoolInUseSize,
                Is.LessThanOrEqualTo(inUseBlockBytes)
            );

            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );
            Assert.That(
                recyclableMemoryStream.MemoryManager.SmallPoolInUseSize,
                Is.LessThanOrEqualTo(inUseBlockBytes)
            );

            var recyclableMemoryStreamManager = recyclableMemoryStream.MemoryManager;
            recyclableMemoryStream.Dispose();
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolInUseSize,
                Is.EqualTo(0)
            );
        }

        [Test]
        public void WriteNullBufferThrowsException()
        {
            var recyclableMemoryStream = GetDefaultStream();
            Assert.Throws<ArgumentNullException>(
                () => recyclableMemoryStream.Write(null, 0, 0)
            );
        }

        [Test]
        public void WriteStartPastBufferThrowsException()
        {
            var recyclableMemoryStream = GetDefaultStream();
            Assert.Throws<ArgumentException>(
                () => recyclableMemoryStream.Write(new byte[] {0, 1}, 2, 1)
            );
        }

        [Test]
        public void WriteStartBeforeBufferThrowsException()
        {
            var recyclableMemoryStream = GetDefaultStream();
            Assert.Throws<ArgumentOutOfRangeException>(
                () => recyclableMemoryStream.Write(new byte[] {0, 1}, -1, 0)
            );
        }

        [Test]
        public void WriteNegativeCountThrowsException()
        {
            var recyclableMemoryStream = GetDefaultStream();
            Assert.Throws<ArgumentOutOfRangeException>(
                () => recyclableMemoryStream.Write(new byte[] {0, 1}, 0, -1)
            );
        }

        [Test]
        public void WriteCountOutOfRangeThrowsException()
        {
            var recyclableMemoryStream = GetDefaultStream();
            Assert.Throws<ArgumentException>(
                () => recyclableMemoryStream.Write(new byte[] {0, 1}, 0, 3))
            ;
        }

        // This is a valid test, but it's too resource-intensive to run on a regular basis.
        //[Test]
        //public void WriteOverflowThrowsException()
        //{
        //    var stream = GetDefaultStream();
        //    int divisor = 256;
        //    var buffer = GetRandomBuffer(Int32.MaxValue / divisor);
        //    Assert.Throws<IOException>(() =>
        //    {
        //        for (int i = 0; i < divisor + 1; i++)
        //        {
        //            stream.Write(buffer, 0, buffer.Length);
        //        }
        //    });
        //}

        [Test]
        public void WriteUpdatesPosition()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var bufferLength = recyclableMemoryStream.MemoryManager.BlockSize / 2 + 1;
            var buffer = GetRandomBuffer(bufferLength);

            for (var i = 0; i < 10; ++i)
            {
                recyclableMemoryStream.Write(
                    buffer,
                    0,
                    bufferLength
                );
                Assert.That(
                    recyclableMemoryStream.Position,
                    Is.EqualTo((i + 1) * bufferLength)
                );
            }
        }

        [Test]
        public void WriteAfterEndIncreasesLength()
        {
            var recyclableMemoryStream = GetDefaultStream();
            const int initialPosition = 13;
            recyclableMemoryStream.Position = initialPosition;

            var buffer = GetRandomBuffer(10);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );
            Assert.That(
                recyclableMemoryStream.Length,
                Is.EqualTo(recyclableMemoryStream.Position)
            );
            Assert.That(
                recyclableMemoryStream.Length,
                Is.EqualTo(initialPosition + buffer.Length)
            );
        }

        [Test]
        public void WritePastMaxStreamLengthThrowsException()
        {
            var recyclableMemoryStream = GetDefaultStream();
            recyclableMemoryStream.Seek(
                Int32.MaxValue,
                SeekOrigin.Begin
            );
            var buffer = GetRandomBuffer(100);
            Assert.Throws<IOException>(
                () => recyclableMemoryStream.Write(buffer, 0, buffer.Length)
            );
        }
        #endregion

        #region WriteByte tests
        [Test]
        public void WriteByteInMiddleSetsCorrectValue()
        {
            var recyclableMemoryStream = GetDefaultStream();

            const int bufferLength = 100;
            var buffer = GetRandomBuffer(bufferLength);
            recyclableMemoryStream.Write(
                buffer,
                0,
                bufferLength
            );
            recyclableMemoryStream.Position = 0;

            var buffer2 = GetRandomBuffer(bufferLength);
            for (var i = 0; i < bufferLength; ++i)
            {
                recyclableMemoryStream.WriteByte(
                    buffer2[i]
                );
            }

            var newBuffer = recyclableMemoryStream.GetBuffer();
            for (var i = 0; i < bufferLength; ++i)
            {
                Assert.That(
                    newBuffer[i],
                    Is.EqualTo(buffer2[i])
                );
            }
        }

        [Test]
        public void WriteByteAtEndSetsCorrectValue()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var buffer = GetRandomBuffer(recyclableMemoryStream.Capacity);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );

            const int testValue = 255;
            recyclableMemoryStream.WriteByte(testValue);
            recyclableMemoryStream.WriteByte(testValue);
            var newBuffer = recyclableMemoryStream.GetBuffer();
            Assert.That(
                newBuffer[buffer.Length],
                Is.EqualTo(testValue)
            );
            Assert.That(
                newBuffer[buffer.Length + 1],
                Is.EqualTo(testValue)
            );
        }

        [Test]
        public void WriteByteAtEndIncreasesLengthByOne()
        {
            var recyclableMemoryStream = GetDefaultStream();
            recyclableMemoryStream.WriteByte(255);
            Assert.That(
                recyclableMemoryStream.Length,
                Is.EqualTo(1)
            );

            recyclableMemoryStream.Position = 0;

            var buffer = GetRandomBuffer(recyclableMemoryStream.Capacity);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );
            Assert.That(
                recyclableMemoryStream.Length,
                Is.EqualTo(buffer.Length)
            );
            recyclableMemoryStream.WriteByte(255);
            Assert.That(
                recyclableMemoryStream.Length,
                Is.EqualTo(buffer.Length + 1)
            );
        }

        [Test]
        public void WriteByteInMiddleDoesNotChangeLength()
        {
            var recyclableMemoryStream = GetDefaultStream();
            const int bufferLength = 100;
            var buffer = GetRandomBuffer(bufferLength);
            recyclableMemoryStream.Write(
                buffer,
                0,
                bufferLength
            );
            Assert.That(
                recyclableMemoryStream.Length,
                Is.EqualTo(bufferLength)
            );
            recyclableMemoryStream.Position = bufferLength / 2;
            recyclableMemoryStream.WriteByte(255);
            Assert.That(
                recyclableMemoryStream.Length,
                Is.EqualTo(bufferLength)
            );
        }

        [Test]
        public void WriteByteDoesNotIncreaseCapacity()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var bufferLength = recyclableMemoryStream.Capacity;
            var buffer = GetRandomBuffer(bufferLength);
            recyclableMemoryStream.Write(
                buffer,
                0,
                bufferLength
            );
            Assert.That(
                recyclableMemoryStream.Capacity,
                Is.EqualTo(bufferLength)
            );

            recyclableMemoryStream.Position = bufferLength / 2;
            recyclableMemoryStream.WriteByte(255);
            Assert.That(
                recyclableMemoryStream.Capacity,
                Is.EqualTo(bufferLength)
            );
        }

        [Test]
        public void WriteByteIncreasesCapacity()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var bufferLength = recyclableMemoryStream.Capacity;
            var buffer = GetRandomBuffer(bufferLength);
            recyclableMemoryStream.Write(
                buffer,
                0,
                bufferLength
            );
            recyclableMemoryStream.WriteByte(255);
            Assert.That(
                recyclableMemoryStream.Capacity,
                Is.EqualTo(2 * bufferLength)
            );
        }

        [Test]
        public void WriteByteUpdatesPosition()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var end = recyclableMemoryStream.Capacity + 1;
            for (var capacityCount = 0; capacityCount < end; capacityCount++)
            {
                recyclableMemoryStream.WriteByte(255);
                Assert.That(
                    recyclableMemoryStream.Position,
                    Is.EqualTo(capacityCount + 1)
                );
            }
        }

        [Test]
        public void WriteByteUpdatesLength()
        {
            var recyclableMemoryStream = GetDefaultStream();
            recyclableMemoryStream.Position = 13;
            recyclableMemoryStream.WriteByte(255);
            Assert.That(
                recyclableMemoryStream.Length,
                Is.EqualTo(14)
            );
        }
        #endregion

        #region SafeReadByte Tests
        [Test]
        public void SafeReadByteDoesNotUpdateStreamPosition()
        {
            var recyclableMemoryStream = GetRandomStream();
            for (int lengthCount = 0; lengthCount < recyclableMemoryStream.Length; lengthCount++)
            {
                var position = lengthCount;
                recyclableMemoryStream.SafeReadByte(ref position);
                Assert.That(
                    position,
                    Is.EqualTo(lengthCount + 1)
                );
                Assert.That(
                    recyclableMemoryStream.Position,
                    Is.EqualTo(0)
                );
            }
        }

        [Test]
        public void SafeReadByteDoesNotDependOnStreamPosition()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var buffer = GetRandomBuffer(recyclableMemoryStream.Capacity * 2);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
                );

            for (var lengthCount = 0; lengthCount < recyclableMemoryStream.Length; lengthCount++)
            {
                recyclableMemoryStream.Position = random.Next(
                    0,
                    buffer.Length - 1
                );
                var position = lengthCount;
                var read = recyclableMemoryStream.SafeReadByte(ref position);
                Assert.That(
                    read,
                    Is.EqualTo(buffer[lengthCount])
                );
                Assert.That(
                    position,
                    Is.EqualTo(lengthCount + 1)
                );
            }
        }

        [Test]
        public void SafeReadByteCanBeUsedInParallel()
        {
            var recyclableMemoryStream = GetDefaultStream();
            const int bufferLength = 1000;
            var buffer = GetRandomBuffer(bufferLength);
            recyclableMemoryStream.Write(
                buffer,
                0,
                bufferLength
            );

            Parallel.For(
                0,
                100,
                i =>
                {
                    for (var lengthCount = 0; lengthCount < bufferLength; lengthCount++)
                    {
                        var position = random.Next(
                            0,
                            bufferLength
                        );
                        var byteRead = recyclableMemoryStream.SafeReadByte(ref position);

                        Assert.That(
                            byteRead,
                            Is.EqualTo(buffer[position - 1])
                        );
                    }
                }
            );
        }
        #endregion

        #region ReadByte Tests
        [Test]
        public void ReadByteUpdatesPosition()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var buffer = GetRandomBuffer(recyclableMemoryStream.Capacity * 2);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );
            recyclableMemoryStream.Position = 0;
            for (var lengthCount = 0; lengthCount < recyclableMemoryStream.Length; lengthCount++)
            {
                recyclableMemoryStream.ReadByte();
                Assert.That(
                    recyclableMemoryStream.Position,
                    Is.EqualTo(lengthCount + 1)
                );
            }
        }

        [Test]
        public void ReadByteAtEndReturnsNegOne()
        {
            var recyclableMemoryStream = GetDefaultStream();
            const int bufferLength = 100;
            var buffer = GetRandomBuffer(bufferLength);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );

            Assert.That(
                recyclableMemoryStream.Position,
                Is.EqualTo(bufferLength)
            );
            Assert.That(
                recyclableMemoryStream.ReadByte(),
                Is.EqualTo(-1)
            );
            Assert.That(
                recyclableMemoryStream.Position,
                Is.EqualTo(bufferLength)
            );
        }

        [Test]
        public void ReadByteReturnsCorrectValueFromBlocks()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var buffer = GetRandomBuffer(recyclableMemoryStream.MemoryManager.BlockSize);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );
            recyclableMemoryStream.Position = 0;
            for (var lengthCount = 0; lengthCount < recyclableMemoryStream.Length; lengthCount++)
            {
                var readByte = recyclableMemoryStream.ReadByte();
                Assert.That(
                    readByte,
                    Is.EqualTo(buffer[lengthCount])
                );
            }
        }

        [Test]
        public void ReadByteReturnsCorrectValueFromLargeBuffer()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var buffer = GetRandomBuffer(recyclableMemoryStream.MemoryManager.LargeBufferMultiple);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );
            recyclableMemoryStream.Position = 0;

            for (var lengthCount = 0; lengthCount < recyclableMemoryStream.Length; lengthCount++)
            {
                var readByte = recyclableMemoryStream.ReadByte();
                Assert.That(
                    readByte,
                    Is.EqualTo(buffer[lengthCount])
                );
                Assert.That(
                    readByte,
                    Is.EqualTo(recyclableMemoryStream.GetBuffer()[lengthCount])
                );
            }
        }
        #endregion

        #region SafeRead Tests
        [Test]
        public void SafeReadDoesNotUpdateStreamPosition()
        {
            var recyclableMemoryStream = GetRandomStream();

            var step = recyclableMemoryStream.MemoryManager.BlockSize / 2;
            var destBuffer = new byte[step];
            var bytesRead = 0;
            var position = 0;

            while (position < recyclableMemoryStream.Length)
            {
                bytesRead += recyclableMemoryStream.SafeRead(
                    destBuffer,
                    0,
                    Math.Min(
                        step,
                        (int)recyclableMemoryStream.Length - bytesRead
                    ),
                    ref position
                );
                Assert.That(
                    position,
                    Is.EqualTo(bytesRead)
                );
                Assert.That(
                    recyclableMemoryStream.Position,
                    Is.EqualTo(0)
                );
            }
        }

        [Test]
        public void SafeReadDoesNotDependOnStreamPosition()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var bufferLength = 1000000;
            var buffer = GetRandomBuffer(bufferLength);
            recyclableMemoryStream.Write(
                buffer,
                0,
                bufferLength
            );

            var step = recyclableMemoryStream.MemoryManager.BlockSize / 2;
            var destBuffer = new byte[step];
            var expected = new byte[step];
            var bytesRead = 0;
            var position = 0;

            while (position < recyclableMemoryStream.Length)
            {
                recyclableMemoryStream.Position = random.Next(
                    0,
                    bufferLength
                );
                var lastPosition = position;
                var lastRead = recyclableMemoryStream.SafeRead(
                    destBuffer,
                    0,
                    Math.Min(
                        step,
                        (int)recyclableMemoryStream.Length - bytesRead
                    ),
                    ref position
                );
                bytesRead += lastRead;

                Array.Copy(
                    buffer,
                    lastPosition,
                    expected,
                    0,
                    lastRead
                );

                Assert.That(
                    position,
                    Is.EqualTo(bytesRead)
                );
                RMSAssert.BuffersAreEqual(
                    destBuffer,
                    expected,
                    lastRead
                );
            }
        }

        [Test]
        public void SafeReadCallsDontAffectOtherSafeReadCalls()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var bufferLength = 1000000;
            var buffer = GetRandomBuffer(bufferLength);
            recyclableMemoryStream.Write(
                buffer,
                0,
                bufferLength
            );

            var stepSlow = recyclableMemoryStream.MemoryManager.BlockSize / 4;
            var stepFast = recyclableMemoryStream.MemoryManager.BlockSize / 2;
            var readBuffer = new byte[stepFast];
            var readSlow = new MemoryStream();
            var readFast = new MemoryStream();

            var positionSlow = 0;
            var positionFast = 0;

            while (positionFast < recyclableMemoryStream.Length)
            {
                var read = recyclableMemoryStream.SafeRead(
                    readBuffer,
                    0,
                    stepFast,
                    ref positionFast
                );
                readFast.Write(
                    readBuffer,
                    0,
                    read
                );
                read = recyclableMemoryStream.SafeRead(
                    readBuffer,
                    0,
                    stepSlow,
                    ref positionSlow
                );
                readSlow.Write(
                    readBuffer,
                    0,
                    read
                );
            }
            while (positionSlow < recyclableMemoryStream.Length)
            {
                var read = recyclableMemoryStream.SafeRead(
                    readBuffer,
                    0,
                    stepSlow,
                    ref positionSlow
                );
                readSlow.Write(
                    readBuffer,
                    0,
                    read
                );
            }

            CollectionAssert.AreEqual(
                readSlow.ToArray(),
                buffer
            );
            CollectionAssert.AreEqual(
                readFast.ToArray(),
                buffer
            );
        }

        [Test]
        public void SafeReadCanBeUsedInParallel()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var bufferLength = 1000000;
            var buffer = GetRandomBuffer(bufferLength);
            recyclableMemoryStream.Write(
                buffer,
                0,
                bufferLength
            );

            Parallel.For(
                0,
                5,
                i =>
                {
                    for (var count = 0; count < 5; count++)
                    {
                        var position = random.Next(
                            0,
                            bufferLength
                        );
                        var startPosition = position;
                        var length = random.Next(
                            0,
                            bufferLength - position
                        );
                        var readBuffer = new byte[length];
                        var bytesRead = recyclableMemoryStream.SafeRead(
                            readBuffer,
                            0,
                            length,
                            ref position
                        );

                        RMSAssert.BuffersAreEqual(
                            readBuffer,
                            0,
                            buffer,
                            startPosition,
                            bytesRead
                        );
                    }
                }
            );
        }
        #endregion

        #region Read tests
        [Test]
        public void ReadNullBufferThrowsException()
        {
            var recyclableMemoryStream = GetDefaultStream();
            Assert.Throws<ArgumentNullException>(
                () => recyclableMemoryStream.Read(null, 0, 1)
            );
        }

        [Test]
        public void ReadNegativeOffsetThrowsException()
        {
            const int bufferLength = 100;
            var recyclableMemoryStream = GetDefaultStream();
            Assert.Throws<ArgumentOutOfRangeException>(
                () => recyclableMemoryStream.Read(new byte[bufferLength], -1, 1)
            );
        }

        [Test]
        public void ReadOffsetPastEndThrowsException()
        {
            const int bufferLength = 100;
            var recyclableMemoryStream = GetDefaultStream();
            Assert.Throws<ArgumentException>(
                () => recyclableMemoryStream.Read(new byte[bufferLength], bufferLength, 1)
            );
        }

        [Test]
        public void ReadNegativeCountThrowsException()
        {
            const int bufferLength = 100;
            var recyclableMemoryStream = GetDefaultStream();
            Assert.Throws<ArgumentOutOfRangeException>(
                () => recyclableMemoryStream.Read(new byte[bufferLength], 0, -1)
            );
        }

        [Test]
        public void ReadCountOutOfBoundsThrowsException()
        {
            const int bufferLength = 100;
            var recyclableMemoryStream = GetDefaultStream();
            Assert.Throws<ArgumentException>(
                () => recyclableMemoryStream.Read(new byte[bufferLength], 0, bufferLength + 1)
            );
        }

        [Test]
        public void ReadOffsetPlusCountLargerThanBufferThrowsException()
        {
            const int bufferLength = 100;
            var recyclableMemoryStream = GetDefaultStream();
            Assert.Throws<ArgumentException>(
                () => recyclableMemoryStream.Read(new byte[bufferLength], bufferLength / 2, bufferLength / 2 + 1)
            );

            var recyclableMemoryStreamRecreate = GetDefaultStream();
            Assert.Throws<ArgumentException>(
                () => recyclableMemoryStreamRecreate.Read(new byte[bufferLength], bufferLength / 2 + 1, bufferLength / 2)
            );
        }

        [Test]
        public void ReadSingleBlockReturnsCorrectBytesReadAndContentsAreCorrect()
        {
            WriteAndReadBytes(DefaultBlockSize);
        }

        [Test]
        public void ReadMultipleBlocksReturnsCorrectBytesReadAndContentsAreCorrect()
        {
            WriteAndReadBytes(DefaultBlockSize * 2);
        }

        protected void WriteAndReadBytes(int length)
        {
            var recyclableMemoryStream = GetDefaultStream();
            var buffer = GetRandomBuffer(length);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );

            recyclableMemoryStream.Position = 0;

            var newBuffer = new byte[buffer.Length];
            var amountRead = recyclableMemoryStream.Read(
                newBuffer,
                0,
                (int)recyclableMemoryStream.Length
            );
            Assert.That(
                amountRead,
                Is.EqualTo(recyclableMemoryStream.Length)
            );
            Assert.That(
                amountRead,
                Is.EqualTo(buffer.Length)
            );

            RMSAssert.BuffersAreEqual(
                buffer,
                newBuffer,
                buffer.Length
            );
        }

        [Test]
        public void ReadFromOffsetHasCorrectLengthAndContents()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var buffer = GetRandomBuffer(100);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );

            recyclableMemoryStream.Position = buffer.Length / 2;
            var amountToRead = buffer.Length / 4;

            var newBuffer = new byte[amountToRead];
            var amountRead = recyclableMemoryStream.Read(
                newBuffer,
                0,
                amountToRead
            );
            Assert.That(
                amountRead,
                Is.EqualTo(amountToRead)
            );
            RMSAssert.BuffersAreEqual(
                buffer,
                buffer.Length / 2,
                newBuffer,
                0,
                amountRead
            );
        }

        [Test]
        public void ReadToOffsetHasCorrectLengthAndContents()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var buffer = GetRandomBuffer(100);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );

            recyclableMemoryStream.Position = 0;
            var newBufferSize = buffer.Length / 2;
            var amountToRead = buffer.Length / 4;
            var offset = newBufferSize - amountToRead;

            var newBuffer = new byte[newBufferSize];
            var amountRead = recyclableMemoryStream.Read(
                newBuffer,
                offset,
                amountToRead
            );
            Assert.That(
                amountRead,
                Is.EqualTo(amountToRead)
            );
            RMSAssert.BuffersAreEqual(
                buffer,
                0,
                newBuffer,
                offset,
                amountRead
            );
        }

        [Test]
        public void ReadFromAndToOffsetHasCorrectLengthAndContents()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var buffer = GetRandomBuffer(100);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );

            recyclableMemoryStream.Position = buffer.Length / 2;
            var newBufferSize = buffer.Length / 2;
            var amountToRead = buffer.Length / 4;
            var offset = newBufferSize - amountToRead;

            var newBuffer = new byte[newBufferSize];
            var amountRead = recyclableMemoryStream.Read(
                newBuffer,
                offset,
                amountToRead
            );
            Assert.That(
                amountRead,
                Is.EqualTo(amountToRead)
            );
            RMSAssert.BuffersAreEqual(
                buffer,
                buffer.Length / 2,
                newBuffer,
                offset,
                amountRead
            );
        }

        [Test]
        public void ReadUpdatesPosition()
        {
            var recyclableMemoryStream = GetDefaultStream();

            const int bufferLength = 1000000;
            var buffer = GetRandomBuffer(bufferLength);
            recyclableMemoryStream.Write(
                buffer,
                0,
                bufferLength
            );

            recyclableMemoryStream.Position = 0;

            var step = recyclableMemoryStream.MemoryManager.BlockSize / 2;
            var destBuffer = new byte[step];
            var bytesRead = 0;
            while (recyclableMemoryStream.Position < recyclableMemoryStream.Length)
            {
                bytesRead += recyclableMemoryStream.Read(
                    destBuffer,
                    0,
                    Math.Min(
                        step,
                        (int)recyclableMemoryStream.Length - bytesRead
                    )
                );
                Assert.That(
                    recyclableMemoryStream.Position,
                    Is.EqualTo(bytesRead)
                );
            }
        }

        [Test]
        public void ReadReturnsEarlyIfLackOfData()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var bufferLength = 100;
            var buffer = GetRandomBuffer(bufferLength);
            recyclableMemoryStream.Write(
                buffer,
                0,
                bufferLength
            );

            recyclableMemoryStream.Position = bufferLength / 2;
            var newBuffer = new byte[bufferLength];
            var amountRead = recyclableMemoryStream.Read(
                newBuffer,
                0,
                bufferLength
            );
            Assert.That(
                amountRead,
                Is.EqualTo(bufferLength / 2)
            );
        }

        [Test]
        public void ReadPastEndOfLargeBufferIsOk()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var bufferLength = recyclableMemoryStream.MemoryManager.LargeBufferMultiple;
            var buffer = GetRandomBuffer(bufferLength);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );

            // Force switch to large buffer
            recyclableMemoryStream.GetBuffer();

            recyclableMemoryStream.Position = recyclableMemoryStream.Length / 2;
            var destBuffer = new byte[bufferLength];
            var amountRead = recyclableMemoryStream.Read(
                destBuffer,
                0,
                destBuffer.Length
            );
            Assert.That(
                amountRead,
                Is.EqualTo(recyclableMemoryStream.Length / 2)
            );
        }

        [Test]
        public void ReadFromPastEndReturnsZero()
        {
            var recyclableMemoryStream = GetDefaultStream();
            const int bufferLength = 100;
            var buffer = GetRandomBuffer(bufferLength);
            recyclableMemoryStream.Write(
                buffer,
                0,
                bufferLength
            );
            recyclableMemoryStream.Position = bufferLength;
            var amountRead = recyclableMemoryStream.Read(
                buffer,
                0,
                bufferLength
            );

            Assert.That(
                amountRead,
                Is.EqualTo(0)
            );
        }
        #endregion

        #region Capacity tests
        [Test]
        public void SetCapacityRoundsUp()
        {
            var recyclableMemoryStream = GetDefaultStream();
            const int step = 51001;
            for (var count = 0; count < 100; count++)
            {
                recyclableMemoryStream.Capacity += step;
                Assert.That(
                    recyclableMemoryStream.Capacity % recyclableMemoryStream.MemoryManager.BlockSize,
                    Is.EqualTo(0)
                );
            }
        }

        [Test]
        public void DecreaseCapacityDoesNothing()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var originalCapacity = recyclableMemoryStream.Capacity;
            recyclableMemoryStream.Capacity *= 2;
            Assert.That(
                recyclableMemoryStream.Capacity,
                Is.GreaterThan(originalCapacity)
            );

            var newCapacity = recyclableMemoryStream.Capacity;
            recyclableMemoryStream.Capacity /= 2;
            Assert.That(
                recyclableMemoryStream.Capacity,
                Is.EqualTo(newCapacity)
            );
        }

        [Test]
        public void CapacityGoesLargeWhenLargeGetBufferCalled()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var buffer = GetRandomBuffer(recyclableMemoryStream.MemoryManager.BlockSize);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );
            Assert.That(
                recyclableMemoryStream.Capacity,
                Is.EqualTo(recyclableMemoryStream.MemoryManager.BlockSize)
            );

            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );
            recyclableMemoryStream.GetBuffer();
            Assert.That(
                recyclableMemoryStream.Capacity,
                Is.EqualTo(recyclableMemoryStream.MemoryManager.LargeBufferMultiple)
            );
        }

        [Test]
        public void EnsureCapacityOperatesOnLargeBufferWhenNeeded()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var buffer = GetRandomBuffer(recyclableMemoryStream.MemoryManager.BlockSize);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );
            recyclableMemoryStream.GetBuffer();

            // At this point, we're not longer using blocks, just large buffers
            Assert.That(
                recyclableMemoryStream.Capacity,
                Is.EqualTo(recyclableMemoryStream.MemoryManager.LargeBufferMultiple)
            );

            // this should bump up the capacity by the LargeBufferMultiple
            recyclableMemoryStream.Capacity = recyclableMemoryStream.MemoryManager.LargeBufferMultiple + 1;

            Assert.That(
                recyclableMemoryStream.Capacity,
                Is.EqualTo(recyclableMemoryStream.MemoryManager.LargeBufferMultiple * 2)
            );
        }
        #endregion

        #region SetLength Tests
        [Test]
        public void SetLengthThrowsExceptionOnNegativeValue()
        {
            var recyclableMemoryStream = GetDefaultStream();
            Assert.Throws<ArgumentOutOfRangeException>(
                () => recyclableMemoryStream.SetLength(-1)
            );
        }

        [Test]
        public void SetLengthSetsLength()
        {
            var recyclableMemoryStream = GetDefaultStream();
            const int length = 100;
            recyclableMemoryStream.SetLength(length);
            Assert.That(
                recyclableMemoryStream.Length,
                Is.EqualTo(length)
            );
        }

        [Test]
        public void SetLengthIncreasesCapacity()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var length = recyclableMemoryStream.Capacity + 1;
            recyclableMemoryStream.SetLength(length);
            Assert.That(
                recyclableMemoryStream.Capacity,
                Is.AtLeast(recyclableMemoryStream.Length)
            );
        }

        [Test]
        public void SetLengthCanSetPosition()
        {
            var recyclableMemoryStream = GetDefaultStream();
            const int length = 100;
            recyclableMemoryStream.SetLength(length);
            recyclableMemoryStream.Position = length / 2;
            Assert.That(
                recyclableMemoryStream.Position,
                Is.EqualTo(length / 2)
            );
        }

        [Test]
        public void SetLengthDoesNotResetPositionWhenGrowing()
        {
            var recyclableMemoryStream = GetDefaultStream();
            const int bufferLength = 100;
            var buffer = GetRandomBuffer(bufferLength);
            recyclableMemoryStream.Write(
                buffer,
                0,
                bufferLength
            );
            recyclableMemoryStream.Position = bufferLength / 4;
            recyclableMemoryStream.SetLength(bufferLength / 2);
            Assert.That(
                recyclableMemoryStream.Position,
                Is.EqualTo(bufferLength / 4)
            );
        }

        [Test]
        public void SetLengthMovesPositionToBeInBounds()
        {
            var recyclableMemoryStream = GetDefaultStream();
            const int bufferLength = 100;
            var buffer = GetRandomBuffer(bufferLength);
            recyclableMemoryStream.Write(
                buffer,
                0,
                bufferLength
            );
            Assert.That(
                recyclableMemoryStream.Position,
                Is.EqualTo(bufferLength)
            );
            recyclableMemoryStream.SetLength(bufferLength / 2);
            Assert.That(
                recyclableMemoryStream.Length,
                Is.EqualTo(bufferLength / 2)
            );
            Assert.That(
                recyclableMemoryStream.Position,
                Is.EqualTo(recyclableMemoryStream.Length)
            );
        }

        [Test]
        public void SetLengthOnTooLargeThrowsException()
        {
            var recyclableMemoryStream = GetDefaultStream();
            Assert.Throws<ArgumentOutOfRangeException>(
                () => recyclableMemoryStream.SetLength((long)Int32.MaxValue + 1)
            );
        }
        #endregion

        #region ToString Tests
        [Test]
        public void ToStringReturnsHelpfulDebugInfo()
        {
            var tag = "Nunit test";
            var recyclableMemoryStream = new RecyclableMemoryStream(GetMemoryManager(), tag);
            var buffer = GetRandomBuffer(1000);
            recyclableMemoryStream.Write(buffer, 0, buffer.Length);
            var debugInfo = recyclableMemoryStream.ToString();

            Assert.That(
                debugInfo,
                Contains.Substring(recyclableMemoryStream.Id.ToString())
            );
            Assert.That(
                debugInfo,
                Contains.Substring(tag)
            );
            Assert.That(
                debugInfo,
                Contains.Substring(buffer.Length.ToString("N0"))
            );
        }

        [Test]
        public void ToStringWithNullTagIsOk()
        {
            var recyclableMemoryStream = new RecyclableMemoryStream(
                GetMemoryManager(),
                null
            );
            var buffer = GetRandomBuffer(1000);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );

            var debugInfo = recyclableMemoryStream.ToString();
            Assert.That(
                debugInfo,
                Contains.Substring(recyclableMemoryStream.Id.ToString())
            );
            Assert.That(
                debugInfo,
                Contains.Substring(buffer.Length.ToString("N0"))
            );
        }
        #endregion

        #region ToArray Tests
        [Test]
        public void ToArrayReturnsDifferentBufferThanGetBufferWithSameContents()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var bufferLength = 100;
            var buffer = GetRandomBuffer(bufferLength);

            recyclableMemoryStream.Write(
                buffer,
                0,
                bufferLength
            );

            var getBuffer = recyclableMemoryStream.GetBuffer();
            var toArrayBuffer = recyclableMemoryStream.ToArray();
            Assert.That(
                toArrayBuffer,
                Is.Not.SameAs(getBuffer)
            );
            RMSAssert.BuffersAreEqual(
                toArrayBuffer,
                getBuffer,
                bufferLength
            );
        }

        [Test]
        public void ToArrayWithLargeBufferReturnsDifferentBufferThanGetBufferWithSameContents()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var bufferLength = recyclableMemoryStream.MemoryManager.BlockSize * 2;
            var buffer = GetRandomBuffer(bufferLength);

            recyclableMemoryStream.Write(
                buffer,
                0,
                bufferLength
            );

            var getBuffer = recyclableMemoryStream.GetBuffer();
            var toArrayBuffer = recyclableMemoryStream.ToArray();
            Assert.That(
                toArrayBuffer,
                Is.Not.SameAs(getBuffer)
            );
            RMSAssert.BuffersAreEqual(
                toArrayBuffer,
                getBuffer,
                bufferLength
            );
        }
        #endregion

        #region CanRead, CanSeek, etc. Tests
        [Test]
        public void CanSeekIsTrue()
        {
            Assert.That(
                GetDefaultStream().CanSeek,
                Is.True
            );
        }

        [Test]
        public void CanReadIsTrue()
        {
            Assert.That(
                GetDefaultStream().CanRead,
                Is.True
            );
        }

        [Test]
        public void CanWriteIsTrue()
        {
            Assert.That(
                GetDefaultStream().CanWrite,
                Is.True
            );
        }

        [Test]
        public void CanTimeutIsFalse()
        {
            Assert.That(
                GetDefaultStream().CanTimeout,
                Is.False
            );
        }
        #endregion

        #region Seek Tests
        [Test]
        public void SeekPastMaximumLengthThrowsException()
        {
            var recyclableMemoryStream = GetDefaultStream();
            Assert.Throws<ArgumentOutOfRangeException>(
                () => recyclableMemoryStream.Seek((long)Int32.MaxValue + 1, SeekOrigin.Begin)
            );
        }

        [Test]
        public void SeekFromBeginToBeforeBeginThrowsException()
        {
            var recyclableMemoryStream = GetDefaultStream();
            Assert.Throws<IOException>(
                () => recyclableMemoryStream.Seek(-1, SeekOrigin.Begin)
            );
        }

        [Test]
        public void SeekFromCurrentToBeforeBeginThrowsException()
        {
            var recyclableMemoryStream = GetDefaultStream();
            Assert.Throws<IOException>(
                () => recyclableMemoryStream.Seek(-1, SeekOrigin.Current)
            );
        }

        [Test]
        public void SeekFromEndToBeforeBeginThrowsException()
        {
            var recyclableMemoryStream = GetDefaultStream();
            Assert.Throws<IOException>(
                () => recyclableMemoryStream.Seek(-1, SeekOrigin.End)
            );
        }

        [Test]
        public void SeekWithBadOriginThrowsException()
        {
            var recyclableMemoryStream = GetDefaultStream();
            Assert.Throws<ArgumentException>(
                () => recyclableMemoryStream.Seek(1, (SeekOrigin)99)
            );
        }

        [Test]
        public void SeekPastEndOfStreamHasCorrectPosition()
        {
            var recyclableMemoryStream = GetDefaultStream();
            const int expected = 100;
            recyclableMemoryStream.Seek(
                expected,
                SeekOrigin.Begin
            );
            Assert.That(
                recyclableMemoryStream.Position,
                Is.EqualTo(expected)
            );
            Assert.That(
                recyclableMemoryStream.Length,
                Is.EqualTo(0)
            );
        }

        [Test]
        public void SeekFromBeginningHasCorrectPosition()
        {
            var recyclableMemoryStream = GetDefaultStream();
            const int position = 100;
            recyclableMemoryStream.Seek(
                position,
                SeekOrigin.Begin
            );
            Assert.That(
                recyclableMemoryStream.Position,
                Is.EqualTo(position)
            );
        }

        [Test]
        public void SeekFromCurrentHasCorrectPosition()
        {
            var recyclableMemoryStream = GetDefaultStream();
            const int position = 100;
            recyclableMemoryStream.Seek(position, SeekOrigin.Begin);
            Assert.That(recyclableMemoryStream.Position, Is.EqualTo(position));

            recyclableMemoryStream.Seek(-100, SeekOrigin.Current);
            Assert.That(recyclableMemoryStream.Position, Is.EqualTo(0));
        }

        [Test]
        public void SeekFromEndHasCorrectPosition()
        {
            var recyclableMemoryStream = GetDefaultStream();
            const int length = 100;
            recyclableMemoryStream.SetLength(length);

            recyclableMemoryStream.Seek(
                -1,
                SeekOrigin.End
            );
            Assert.That(
                recyclableMemoryStream.Position,
                Is.EqualTo(length - 1)
            );
        }

        [Test]
        public void SeekPastEndAndWriteHasCorrectLengthAndPosition()
        {
            var recyclableMemoryStream = GetDefaultStream();
            const int position = 100;
            const int bufferLength = 100;
            recyclableMemoryStream.Seek(
                position,
                SeekOrigin.Begin
            );
            var buffer = GetRandomBuffer(bufferLength);
            recyclableMemoryStream.Write(
                buffer,
                0,
                bufferLength
            );
            Assert.That(
                recyclableMemoryStream.Length,
                Is.EqualTo(position + bufferLength)
            );
            Assert.That(
                recyclableMemoryStream.Position,
                Is.EqualTo(position + bufferLength)
            );
        }
        #endregion

        #region Position Tests
        [Test]
        public void PositionSetToNegativeThrowsException()
        {
            var recyclableMemoryStream = GetDefaultStream();
            Assert.Throws<ArgumentOutOfRangeException>(
                () => recyclableMemoryStream.Position = -1
            );
        }

        [Test]
        public void PositionSetToLargerThanMaxStreamLengthThrowsException()
        {
            var recyclableMemoryStream = GetDefaultStream();
            Assert.Throws<ArgumentOutOfRangeException>(
                () => recyclableMemoryStream.Position = (long)Int32.MaxValue + 1
            );
        }

        [Test]
        public void PositionSetToAnyValue()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var maxValue = Int32.MaxValue;
            var step = maxValue / 32;
            for (long positionCount = 0; positionCount < maxValue; positionCount += step)
            {
                recyclableMemoryStream.Position = positionCount;
                Assert.That(
                    recyclableMemoryStream.Position,
                    Is.EqualTo(positionCount)
                );
            }
        }
        #endregion

        #region Dispose and Pooling Tests
        [Test]
        public void Pooling_NewMemoryManagerHasZeroFreeAndInUseBytes()
        {
            var recyclableMemoryStreamManager = GetMemoryManager();
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolFreeSize,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.LargePoolFreeSize,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolInUseSize,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.LargePoolInUseSize,
                Is.EqualTo(0)
            );
        }

        [Test]
        public void Pooling_NewStreamIncrementsInUseBytes()
        {
            var recyclableMemoryStreamManager = GetMemoryManager();
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolInUseSize,
                Is.EqualTo(0)
            );

            var recyclableMemoryStream = new RecyclableMemoryStream(recyclableMemoryStreamManager);
            Assert.That(
                recyclableMemoryStream.Capacity,
                Is.EqualTo(recyclableMemoryStreamManager.BlockSize)
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolInUseSize,
                Is.EqualTo(recyclableMemoryStreamManager.BlockSize)
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolFreeSize,
                Is.EqualTo(0)
            );
        }

        [Test]
        public void Pooling_DisposeOneBlockAdjustsInUseAndFreeBytes()
        {
            var recyclableMemoryStream = GetDefaultStream();
            Assert.That(
                recyclableMemoryStream.MemoryManager.SmallPoolInUseSize,
                Is.EqualTo(recyclableMemoryStream.Capacity)
            );

            var recyclableMemoryStreamManager = recyclableMemoryStream.MemoryManager;
            recyclableMemoryStream.Dispose();
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolInUseSize,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolFreeSize,
                Is.EqualTo(recyclableMemoryStreamManager.BlockSize)
            );
        }

        [Test]
        public void Pooling_DisposeMultipleBlocksAdjustsInUseAndFreeBytes()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var recyclableMemoryStreamManager = recyclableMemoryStream.MemoryManager;
            var bufferLength = recyclableMemoryStreamManager.BlockSize * 4;
            var buffer = GetRandomBuffer(bufferLength);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );

            Assert.That(
                recyclableMemoryStreamManager.SmallPoolInUseSize,
                Is.EqualTo(bufferLength)
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolFreeSize,
                Is.EqualTo(0)
            );
            recyclableMemoryStream.Dispose();

            Assert.That(
                recyclableMemoryStreamManager.SmallPoolInUseSize,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolFreeSize,
                Is.EqualTo(bufferLength)
            );
        }

        [Test]
        public void Pooling_DisposingFreesBlocks()
        {
            var recyclableMemoryStream = GetDefaultStream();
            const int numBlocks = 4;
            var bufferLength = recyclableMemoryStream.MemoryManager.BlockSize * numBlocks;
            var buffer = GetRandomBuffer(bufferLength);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );
            var recyclableMemoryStreamManager = recyclableMemoryStream.MemoryManager;
            recyclableMemoryStream.Dispose();
            Assert.That(
                recyclableMemoryStreamManager.SmallBlocksFree,
                Is.EqualTo(numBlocks)
            );
        }

        [Test]
        public void DisposeReturnsLargeBuffer()
        {
            var recyclableMemoryStream = GetDefaultStream();
            const int numBlocks = 4;
            var bufferLength = recyclableMemoryStream.MemoryManager.BlockSize * numBlocks;
            var buffer = GetRandomBuffer(bufferLength);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );
            var newBuffer = recyclableMemoryStream.GetBuffer();
            Assert.That(
                newBuffer.Length,
                Is.EqualTo(recyclableMemoryStream.MemoryManager.LargeBufferMultiple)
            );

            var recyclableMemoryStreamManager = recyclableMemoryStream.MemoryManager;
            Assert.That(
                recyclableMemoryStreamManager.LargeBuffersFree,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.LargePoolFreeSize,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.LargePoolInUseSize,
                Is.EqualTo(newBuffer.Length)
            );
            recyclableMemoryStream.Dispose();
            Assert.That(
                recyclableMemoryStreamManager.LargeBuffersFree,
                Is.EqualTo(1)
            );
            Assert.That(
                recyclableMemoryStreamManager.LargePoolFreeSize,
                Is.EqualTo(newBuffer.Length)
            );
            Assert.That(
                recyclableMemoryStreamManager.LargePoolInUseSize, 
                Is.EqualTo(0)
            );
        }

        [Test]
        public void DisposeTwiceDoesNotThrowException()
        {
            var recyclableMemoryStream = GetDefaultStream();
            recyclableMemoryStream.Dispose();
            recyclableMemoryStream.Dispose();
        }

        [Test]
        public async Task ConcurrentDoubleDisposeSucceeds()
        {
            const int blockSize = 10;
            var recyclableMemoryStreamManager = new RecyclableMemoryStreamManager(
                blockSize: blockSize,
                largeBufferMultiple: 20,
                maximumBufferSize: 160,
                useExponentialLargeBuffer: useExponentialLargeBuffer
            );
            RecyclableMemoryStream recyclableMemoryStream = new RecyclableMemoryStream(
                recyclableMemoryStreamManager,
                TestContext.CurrentContext.Test.Name
            );

            Assert.AreEqual(
                0,
                recyclableMemoryStreamManager.SmallBlocksFree,
                "Verify manager starts with no blocks free"
            );
            Assert.AreEqual(
                0,
                recyclableMemoryStreamManager.SmallPoolFreeSize,
                "Verify manager reports no size for free blocks"
            );
            Assert.AreEqual(
                blockSize,
                recyclableMemoryStreamManager.SmallPoolInUseSize,
                "Verify manager gave RMS one block"
            );

            byte[] data = GetRandomBuffer(length: 100);
            recyclableMemoryStream.Write(
                data,
                0,
                data.Length
            );

            Assert.AreEqual(
                0,
                recyclableMemoryStreamManager.SmallBlocksFree,
                "Verify manager has no blocks free after stream was written to"
            );
            Assert.AreEqual(
                0,
                recyclableMemoryStreamManager.SmallPoolFreeSize,
                "Verify manager reports no size for free blocks after stream was written to"
            );
            Assert.AreEqual(
                data.Length,
                recyclableMemoryStreamManager.SmallPoolInUseSize,
                "Verify manager gave the stream the correct amount of blocks based on the write"
            );

            var recyclableMemoryStreamEventListener = new RecyclableMemoryStreamEventListener();
            Assert.IsFalse(
                recyclableMemoryStreamEventListener.MemoryStreamDoubleDisposeCalled
            );

            using (recyclableMemoryStreamEventListener)
            {
                Task dispose1 = Task.Run(
                    () => recyclableMemoryStream.Dispose()
                );
                Task dispose2 = Task.Run(
                    () => recyclableMemoryStream.Dispose()
                );
                await Task.WhenAll(
                    dispose1,
                    dispose2
                );
                
                Assert.AreEqual(
                    data.Length / blockSize,
                    recyclableMemoryStreamManager.SmallBlocksFree,
                    "Verify manager has correct free blocks after double dispose"
                );
                Assert.AreEqual(
                    data.Length,
                    recyclableMemoryStreamManager.SmallPoolFreeSize,
                    "Verify manager reports correct free pool size after double dispose"
                );
                Assert.AreEqual(
                    0,
                    recyclableMemoryStreamManager.SmallPoolInUseSize,
                    "Verify manager reports the correct pool usage size after double dispose"
                );
            }

            Assert.IsTrue(recyclableMemoryStreamEventListener.MemoryStreamDoubleDisposeCalled);
        }

        /*
         * TODO: clocke to release logging libraries to enable some tests.
        [Test]
        public void DisposeTwiceRecordsCallstackInLog()
        {
            var logger = LogManager.CreateMemoryLogger();
            logger.SubscribeToEvents(Events.Writer, EventLevel.Verbose);

            try
            {
                var stream = GetDefaultStream();
                stream.MemoryManager.GenerateCallStacks = true;

                stream.Dispose();
                stream.Dispose();
                Assert.Fail("Did not throw exception as expected");
            }
            catch (InvalidOperationException)
            {
                // wait for log to flush
                GC.Collect(1);
                GC.WaitForPendingFinalizers();
                Thread.Sleep(250);

                var log = Encoding.UTF8.GetString(logger.Stream.GetBuffer(), 0, (int)logger.Stream.Length);
                Assert.That(log, Is.StringContaining("MemoryStreamDoubleDispose"));
                Assert.That(log, Is.StringContaining("RecyclableMemoryStream.Dispose("));
                Assert.That(log, Is.StringContaining("disposeStack1=\" "));
                Assert.That(log, Is.StringContaining("disposeStack2=\" "));
            }
        }
        */

        [Test]
        public void DisposeReturningATooLargeBufferGetsDropped()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var recyclableMemoryStreamManager = recyclableMemoryStream.MemoryManager;
            var bufferSize = recyclableMemoryStreamManager.MaximumBufferSize + 1;
            var buffer = GetRandomBuffer(bufferSize);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );

            var newBuffer = recyclableMemoryStream.GetBuffer();
            Assert.That(
                recyclableMemoryStreamManager.LargePoolInUseSize,
                Is.EqualTo(newBuffer.Length)
            );
            Assert.That(
                recyclableMemoryStreamManager.LargePoolFreeSize,
                Is.EqualTo(0)
            );

            recyclableMemoryStream.Dispose();
            Assert.That(
                recyclableMemoryStreamManager.LargePoolInUseSize,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.LargePoolFreeSize,
                Is.EqualTo(0)
            );
        }

        [Test]
        public void AccessingObjectAfterDisposeThrowsObjectDisposedException()
        {
            var recyclableMemoryStream = GetDefaultStream();
            recyclableMemoryStream.Dispose();

            Assert.That(
                recyclableMemoryStream.CanRead,
                Is.False
            );
            Assert.That(
                recyclableMemoryStream.CanSeek,
                Is.False
            );
            Assert.That(
                recyclableMemoryStream.CanWrite,
                Is.False
            );

            var buffer = new byte[100];
            Assert.Throws<ObjectDisposedException>(
                () =>
                {
                    var x = recyclableMemoryStream.Capacity;
                }
            );
            Assert.Throws<ObjectDisposedException>(
                () =>
                {
                    var x = recyclableMemoryStream.Length;
                }
            );
            Assert.Throws<ObjectDisposedException>(
                () =>
                {
                    var x = recyclableMemoryStream.MemoryManager;
                }
            );
            Assert.Throws<ObjectDisposedException>(
                () =>
                {
                    var x = recyclableMemoryStream.Id;
                }
            );
            Assert.Throws<ObjectDisposedException>(
                () =>
                {
                    var x = recyclableMemoryStream.Tag;
                }
            );
            Assert.Throws<ObjectDisposedException>(
                () =>
                {
                    var x = recyclableMemoryStream.Position;
                }
            );
            Assert.Throws<ObjectDisposedException>(
                () =>
                {
                    var x = recyclableMemoryStream.ReadByte();
                }
            );
            Assert.Throws<ObjectDisposedException>(
                () =>
                {
                    var x = recyclableMemoryStream.Read(
                        buffer,
                        0,
                        buffer.Length
                    );
                }
            );
            Assert.Throws<ObjectDisposedException>(
                () =>
                {
                    recyclableMemoryStream.WriteByte(255);
                }
            );
            Assert.Throws<ObjectDisposedException>(
                () =>
                {
                    recyclableMemoryStream.Write(
                        buffer,
                        0,
                        buffer.Length
                    );
                }
            );
            Assert.Throws<ObjectDisposedException>(
                () =>
                {
                    recyclableMemoryStream.SetLength(100);
                }
            );
            Assert.Throws<ObjectDisposedException>(
                () =>
                {
                    recyclableMemoryStream.Seek(
                        0,
                        SeekOrigin.Begin
                    );
                }
            );
            Assert.Throws<ObjectDisposedException>(
                () =>
                {
                    var x = recyclableMemoryStream.ToArray();
                }
            );
            Assert.Throws<ObjectDisposedException>(
                () =>
                {
                    var x = recyclableMemoryStream.GetBuffer();
                }
            );
        }
        #endregion

        #region GetStream tests
        [Test]
        public void GetStreamReturnsADefaultStream()
        {
            var recyclableMemoryStreamManager = GetMemoryManager();
            var recyclableMemoryStream = recyclableMemoryStreamManager.GetStream();
            Assert.That(
                recyclableMemoryStream.Capacity,
                Is.EqualTo(recyclableMemoryStreamManager.BlockSize)
            );
        }

        [Test]
        public void GetStreamWithTag()
        {
            var recyclableMemoryStreamManager = GetMemoryManager();
            const string tag = "MyTag";
            var recyclableMemoryStream = recyclableMemoryStreamManager.GetStream(tag) as RecyclableMemoryStream;
            Assert.That(
                recyclableMemoryStream.Capacity,
                Is.EqualTo(recyclableMemoryStreamManager.BlockSize)
            );
            Assert.That(
                recyclableMemoryStream.Tag,
                Is.EqualTo(tag)
            );
        }

        [Test]
        public void GetStreamWithTagAndRequiredSize()
        {
            var recyclableMemoryStreamManager = GetMemoryManager();
            const string tag = "MyTag";
            var requiredSize = 13131313;
            var recyclableMemoryStream = recyclableMemoryStreamManager.GetStream(
                tag,
                requiredSize
            ) as RecyclableMemoryStream;
            Assert.That(
                recyclableMemoryStream.Capacity,
                Is.AtLeast(requiredSize)
            );
            Assert.That(
                recyclableMemoryStream.Tag,
                Is.EqualTo(tag)
            );
        }

        [Test]
        public void GetStreamWithTagAndRequiredSizeAndContiguousBuffer()
        {
            var recyclableMemoryStreamManager = GetMemoryManager();
            const string tag = "MyTag";
            const int requiredSize = 13131313;

            var recyclableMemoryStream = recyclableMemoryStreamManager.GetStream(
                tag,
                requiredSize,
                false
            ) as RecyclableMemoryStream;
            Assert.That(
                recyclableMemoryStream.Capacity,
                Is.AtLeast(requiredSize)
            );
            Assert.That(
                recyclableMemoryStream.Tag,
                Is.EqualTo(tag)
            );

            var recyclableMemoryStreamRecreate = recyclableMemoryStreamManager.GetStream(
                tag,
                requiredSize,
                true
            ) as RecyclableMemoryStream;
            Assert.That(
                recyclableMemoryStreamRecreate.Capacity,
                Is.AtLeast(requiredSize)
            );
            Assert.That(
                recyclableMemoryStreamRecreate.Tag,
                Is.EqualTo(tag)
            );
        }

        [Test]
        public void GetStreamWithBuffer()
        {
            var recyclableMemoryStreamManager = GetMemoryManager();
            var buffer = GetRandomBuffer(1000);
            const string tag = "MyTag";

            var recyclableMemoryStream = recyclableMemoryStreamManager.GetStream(
                tag,
                buffer,
                1,
                buffer.Length - 1
            ) as RecyclableMemoryStream;
            RMSAssert.BuffersAreEqual(
                buffer,
                1,
                recyclableMemoryStream.GetBuffer(),
                0,
                buffer.Length - 1
            );
            Assert.That(
                buffer,
                Is.Not.SameAs(recyclableMemoryStream.GetBuffer())
            );
            Assert.That(
                recyclableMemoryStream.Tag,
                Is.EqualTo(tag)
            );
        }
        #endregion

        #region WriteTo tests
        [Test]
        public void WriteToNullStreamThrowsException()
        {
            var recyclableMemoryStream = GetDefaultStream();
            Assert.Throws<ArgumentNullException>(
                () => recyclableMemoryStream.WriteTo(null)
            );
        }

        [Test]
        public void WriteToOtherStreamHasEqualsContents()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var buffer = GetRandomBuffer(100);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );

            var recyclableMemoryStreamRecreate = GetDefaultStream();
            recyclableMemoryStream.WriteTo(recyclableMemoryStreamRecreate);

            Assert.That(
                recyclableMemoryStreamRecreate.Length,
                Is.EqualTo(recyclableMemoryStream.Length)
            );
            RMSAssert.BuffersAreEqual(
                buffer,
                recyclableMemoryStreamRecreate.GetBuffer(),
                buffer.Length
            );
        }
        #endregion

        #region MaximumStreamCapacityBytes Tests
        [Test]
        public void MaximumStreamCapacity_NoLimit()
        {
            var recyclableMemoryStream = GetDefaultStream();
            recyclableMemoryStream.MemoryManager.MaximumStreamCapacity = 0;
            recyclableMemoryStream.Capacity = (DefaultMaximumBufferSize * 2) + 1;
            Assert.That(
                recyclableMemoryStream.Capacity,
                Is.AtLeast((DefaultMaximumBufferSize * 2) + 1)
            );
        }

        [Test]
        public void MaximumStreamCapacity_Limit()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var maxCapacity = DefaultMaximumBufferSize * 2;
            recyclableMemoryStream.MemoryManager.MaximumStreamCapacity = maxCapacity;
            Assert.DoesNotThrow(
                () => recyclableMemoryStream.Capacity = maxCapacity
            );
            Assert.Throws<InvalidOperationException>(
                () => recyclableMemoryStream.Capacity = maxCapacity + 1
            );
        }

        [Test]
        public void MaximumStreamCapacity_StreamUnchanged()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var maxCapacity = DefaultMaximumBufferSize * 2;
            recyclableMemoryStream.MemoryManager.MaximumStreamCapacity = maxCapacity;
            Assert.DoesNotThrow(
                () => recyclableMemoryStream.Capacity = maxCapacity
            );
            var oldCapacity = recyclableMemoryStream.Capacity;
            Assert.Throws<InvalidOperationException>(
                () => recyclableMemoryStream.Capacity = maxCapacity + 1
            );
            Assert.That(
                recyclableMemoryStream.Capacity,
                Is.EqualTo(oldCapacity)
            );
        }

        [Test]
        public void MaximumStreamCapacity_StreamUnchangedAfterWriteOverLimit()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var maxCapacity = DefaultMaximumBufferSize * 2;
            recyclableMemoryStream.MemoryManager.MaximumStreamCapacity = maxCapacity;
            var buffer1 = GetRandomBuffer(100);
            recyclableMemoryStream.Write(
                buffer1,
                0,
                buffer1.Length
            );
            var oldLength = recyclableMemoryStream.Length;
            var oldCapacity = recyclableMemoryStream.Capacity;
            var oldPosition = recyclableMemoryStream.Position;
            var buffer2 = GetRandomBuffer(maxCapacity);
            Assert.Throws<InvalidOperationException>(
                () => recyclableMemoryStream.Write(buffer2, 0, buffer2.Length)
            );
            Assert.That(
                recyclableMemoryStream.Length,
                Is.EqualTo(oldLength)
            );
            Assert.That(
                recyclableMemoryStream.Capacity,
                Is.EqualTo(oldCapacity)
            );
            Assert.That(
                recyclableMemoryStream.Position,
                Is.EqualTo(oldPosition)
            );
        }
        #endregion

        #region Test Helpers
        protected RecyclableMemoryStream GetDefaultStream()
        {
            return new RecyclableMemoryStream(GetMemoryManager());
        }

        protected byte[] GetRandomBuffer(int length)
        {
            var buffer = new byte[length];
            for (var i = 0; i < buffer.Length; ++i)
            {
                buffer[i] = (byte)random.Next(
                    byte.MinValue,
                    byte.MaxValue + 1
                );
            }
            return buffer;
        }

        protected virtual RecyclableMemoryStreamManager GetMemoryManager()
        {
            return new RecyclableMemoryStreamManager(
                DefaultBlockSize,
                DefaultLargeBufferMultiple,
                DefaultMaximumBufferSize,
                useExponentialLargeBuffer
            )
            {
                AggressiveBufferReturn = AggressiveBufferRelease,
            };
        }

        private RecyclableMemoryStream GetRandomStream()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var buffer = GetRandomBuffer(recyclableMemoryStream.Capacity * 2);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );
            recyclableMemoryStream.Position = 0;
            return recyclableMemoryStream;
        }
        #endregion

        protected abstract bool AggressiveBufferRelease { get; }

        protected virtual bool useExponentialLargeBuffer
        {
            get { return false; }
        }

        /*
         * TODO: clocke to release logging libraries to enable some tests.
        [TestFixtureSetUp]
        public void Setup()
        {
            LogManager.Start();
        }
        */

        protected static class RMSAssert
        {
            /// <summary>
            /// Asserts that two buffers are euqual, up to the given count
            /// </summary>
            internal static void BuffersAreEqual(byte[] buffer1, byte[] buffer2, int count)
            {
                BuffersAreEqual(
                    buffer1,
                    0,
                    buffer2,
                    0,
                    count
                );
            }

            /// <summary>
            /// Asserts that two buffers are equal, up to the given count, starting at the specific offsets for each buffer
            /// </summary>
            internal static void BuffersAreEqual(byte[] buffer1, int offset1, byte[] buffer2, int offset2, int count, double tolerance = 0.0)
            {
                if (null == buffer1 && null == buffer2)
                {
                    //If both null, it's ok
                    return;
                }

                // If either one is null, we fail
                Assert.That(
                    (null != buffer1) && (null != buffer2),
                    Is.True
                );

                Assert.That(
                    buffer1.Length - offset1 >= count
                );

                Assert.That(
                    buffer2.Length - offset2 >= count
                );

                var errors = 0;
                for (int i1 = offset1, i2 = offset2; i1 < offset1 + count; ++i1, ++i2)
                {
                    if (0.0 == tolerance)
                    {
                        Assert.That(
                            buffer1[i1],
                            Is.EqualTo(buffer2[i2]),
                            $"Buffers are different. buffer1[{i1}]=={buffer1[i1]}, buffer2[{i2}]=={buffer2[i2]}"
                        );
                    }
                    else
                    {
                        if (buffer1[i1] != buffer2[i2])
                        {
                            errors++;
                        }
                    }
                }
                var rate = (double)errors / count;
                Assert.That(
                    rate,
                    Is.AtMost(tolerance),
                    $"Too many errors. Buffers can differ to a tolerance of {tolerance:F4}"
                );
            }
        }
    }

    [TestFixture]
    public sealed class RecyclableMemoryStreamTestsWithPassiveBufferRelease : BaseRecyclableMemoryStreamTests
    {
        protected override bool AggressiveBufferRelease
        {
            get { return false; }
        }

        [Test]
        public void OldBuffersAreKeptInStreamUntilDispose()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var recyclableMemoryStreamManager = recyclableMemoryStream.MemoryManager;
            var buffer = GetRandomBuffer(recyclableMemoryStream.MemoryManager.LargeBufferMultiple);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );
            recyclableMemoryStream.GetBuffer();

            Assert.That(
                recyclableMemoryStreamManager.LargePoolInUseSize,
                Is.EqualTo(recyclableMemoryStreamManager.LargeBufferMultiple * (1))
            );
            Assert.That(
                recyclableMemoryStreamManager.LargePoolFreeSize,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolFreeSize,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolInUseSize,
                Is.EqualTo(recyclableMemoryStreamManager.LargeBufferMultiple)
            );

            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );

            Assert.That(
                recyclableMemoryStreamManager.LargePoolFreeSize,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.LargePoolInUseSize,
                Is.EqualTo(recyclableMemoryStreamManager.LargeBufferMultiple * (1 + 2))
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolFreeSize,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolInUseSize,
                Is.EqualTo(recyclableMemoryStreamManager.LargeBufferMultiple)
            );

            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );

            Assert.That(
                recyclableMemoryStreamManager.LargePoolFreeSize,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.LargePoolInUseSize,
                Is.EqualTo(recyclableMemoryStreamManager.LargeBufferMultiple * (1 + 2 + 3))
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolFreeSize,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolInUseSize,
                Is.EqualTo(recyclableMemoryStreamManager.LargeBufferMultiple)
            );

            recyclableMemoryStream.Dispose();

            Assert.That(
                recyclableMemoryStreamManager.LargePoolFreeSize,
                Is.EqualTo(recyclableMemoryStreamManager.LargeBufferMultiple * (1 + 2 + 3))
            );
            Assert.That(
                recyclableMemoryStreamManager.LargePoolInUseSize,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolFreeSize,
                Is.EqualTo(recyclableMemoryStreamManager.LargeBufferMultiple)
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolInUseSize,
                Is.EqualTo(0)
            );
        }
    }

    [TestFixture]
    public sealed class RecyclableMemoryStreamTestsWithAggressiveBufferRelease : BaseRecyclableMemoryStreamTests
    {
        protected override bool AggressiveBufferRelease
        {
            get { return true; }
        }
    }

    public abstract class BaseRecyclableMemoryStreamTestsUsingExponentialLargeBuffer : BaseRecyclableMemoryStreamTests
    {
        protected override bool useExponentialLargeBuffer
        {
            get { return true; }
        }

        [Test]
        public override void RecyclableMemoryManagerUsingMultipleOrExponentialLargeBuffer()
        {
            var recyclableMemoryStreamManager = GetMemoryManager();
            Assert.That(
                recyclableMemoryStreamManager.UseMultipleLargeBuffer,
                Is.False
            );
            Assert.That(
                recyclableMemoryStreamManager.UseExponentialLargeBuffer,
                Is.True
            );
        }

        [Test]
        public override void RecyclableMemoryManagerThrowsExceptionOnMaximumBufferNotMultipleOrExponentialOfLargeBufferMultiple()
        {
            Assert.Throws<ArgumentException>(
                () => new RecyclableMemoryStreamManager(100, 1024, 2025, useExponentialLargeBuffer)
            );
            Assert.Throws<ArgumentException>(
                () => new RecyclableMemoryStreamManager(100, 1024, 2023, useExponentialLargeBuffer)
            );
            Assert.Throws<ArgumentException>(
                () => new RecyclableMemoryStreamManager(100, 1024, 3072, useExponentialLargeBuffer)
            );
            Assert.DoesNotThrow(
                () => new RecyclableMemoryStreamManager(100, 1024, 2048, useExponentialLargeBuffer)
            );
            Assert.DoesNotThrow(
                () => new RecyclableMemoryStreamManager(100, 1024, 4096, useExponentialLargeBuffer)
            );
        }

        [Test]
        public override void GetLargeBufferAlwaysAMultipleOrExponentialOfMegabyteAndAtLeastAsMuchAsRequestedForLargeBuffer()
        {
            const int step = 200000;
            const int start = 1;
            const int end = 16000000;
            var recyclableMemoryStreamManager = GetMemoryManager();

            for (var count = start; count <= end; count += step)
            {
                var buffer = recyclableMemoryStreamManager.GetLargeBuffer(
                    count,
                    DefaultTag
                );
                Assert.That(
                    buffer.Length >= count,
                    Is.True
                );
                Assert.That(
                    recyclableMemoryStreamManager.LargeBufferMultiple * (int)Math.Pow(2, Math.Floor(Math.Log(buffer.Length / recyclableMemoryStreamManager.LargeBufferMultiple, 2))) == buffer.Length,
                    Is.True,
                    $"buffer length of {buffer.Length} is not a exponential of {recyclableMemoryStreamManager.LargeBufferMultiple}"
                );
            }
        }

        [Test]
        public override void AllMultiplesOrExponentialUpToMaxCanBePooled()
        {
            const int BlockSize = 100;
            const int LargeBufferMultiple = 1000;
            const int MaxBufferSize = 8000;

            for (var size = LargeBufferMultiple; size <= MaxBufferSize; size *= 2)
            {
                var recyclableMemoryStreamManager = new RecyclableMemoryStreamManager(
                    BlockSize,
                    LargeBufferMultiple, MaxBufferSize, useExponentialLargeBuffer
                )
                {
                    AggressiveBufferReturn = AggressiveBufferRelease
                };
                var buffer = recyclableMemoryStreamManager.GetLargeBuffer(
                    size,
                    DefaultTag
                );
                Assert.That(
                    recyclableMemoryStreamManager.LargePoolFreeSize,
                    Is.EqualTo(0)
                );
                Assert.That(
                    recyclableMemoryStreamManager.LargePoolInUseSize,
                    Is.EqualTo(size)
                );

                recyclableMemoryStreamManager.ReturnLargeBuffer(
                    buffer,
                    DefaultTag
                );

                Assert.That(
                    recyclableMemoryStreamManager.LargePoolFreeSize,
                    Is.EqualTo(size)
                );
                Assert.That(
                    recyclableMemoryStreamManager.LargePoolInUseSize,
                    Is.EqualTo(0)
                );
            }
        }

        [Test]
        public override void RequestTooLargeBufferAdjustsInUseCounter()
        {
            var recyclableMemoryStreamManager = GetMemoryManager();
            var buffer = recyclableMemoryStreamManager.GetLargeBuffer(
                recyclableMemoryStreamManager.MaximumBufferSize + 1,
                DefaultTag
            );
            Assert.That(
                buffer.Length,
                Is.EqualTo(recyclableMemoryStreamManager.MaximumBufferSize * 2)
            );
            Assert.That(
                recyclableMemoryStreamManager.LargePoolInUseSize,
                Is.EqualTo(buffer.Length)
            );
        }

        protected override void TestDroppingLargeBuffer(long maxFreeLargeBufferSize)
        {
            const int BlockSize = 100;
            const int LargeBufferMultiple = 1000;
            const int MaxBufferSize = 8000;

            for (var size = LargeBufferMultiple; size <= MaxBufferSize; size *= 2)
            {
                var recyclableMemoryStreamManager = new RecyclableMemoryStreamManager(
                    BlockSize,
                    LargeBufferMultiple,
                    MaxBufferSize,
                    useExponentialLargeBuffer
                )
                {
                    AggressiveBufferReturn = AggressiveBufferRelease,
                    MaximumFreeLargePoolBytes = maxFreeLargeBufferSize
                };

                var buffers = new List<byte[]>();

                //Get one extra buffer
                var buffersToRetrieve = (maxFreeLargeBufferSize > 0)
                                        ? (maxFreeLargeBufferSize / size + 1)
                                        : 10;
                for (var i = 0; i < buffersToRetrieve; i++)
                {
                    var buffer = recyclableMemoryStreamManager.GetLargeBuffer(
                        size,
                        DefaultTag
                    );
                    buffers.Add(buffer);
                }

                Assert.That(
                    recyclableMemoryStreamManager.LargePoolInUseSize,
                    Is.EqualTo(size * buffersToRetrieve)
                );
                Assert.That(
                    recyclableMemoryStreamManager.LargePoolFreeSize,
                    Is.EqualTo(0)
                );
                foreach (var buffer in buffers)
                {
                    recyclableMemoryStreamManager.ReturnLargeBuffer(
                        buffer,
                        DefaultTag
                    );
                }
                Assert.That(
                    recyclableMemoryStreamManager.LargePoolInUseSize,
                    Is.EqualTo(0)
                );
                if (maxFreeLargeBufferSize > 0)
                {
                    Assert.That(
                        recyclableMemoryStreamManager.LargePoolFreeSize,
                        Is.LessThanOrEqualTo(maxFreeLargeBufferSize)
                    );
                }
                else
                {
                    Assert.That(
                        recyclableMemoryStreamManager.LargePoolFreeSize,
                        Is.EqualTo(buffersToRetrieve * size)
                    );
                }
            }
        }
    }

    [TestFixture]
    public sealed class RecyclableMemoryStreamTestsWithPassiveBufferReleaseUsingExponentialLargeBuffer : BaseRecyclableMemoryStreamTestsUsingExponentialLargeBuffer
    {
        protected override bool AggressiveBufferRelease
        {
            get { return false; }
        }

        [Test]
        public void OldBuffersAreKeptInStreamUntilDispose()
        {
            var recyclableMemoryStream = GetDefaultStream();
            var recyclableMemoryStreamManager = recyclableMemoryStream.MemoryManager;
            var buffer = GetRandomBuffer(recyclableMemoryStream.MemoryManager.LargeBufferMultiple);
            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );
            recyclableMemoryStream.GetBuffer();

            Assert.That(
                recyclableMemoryStreamManager.LargePoolInUseSize,
                Is.EqualTo(recyclableMemoryStreamManager.LargeBufferMultiple * (1))
            );
            Assert.That(
                recyclableMemoryStreamManager.LargePoolFreeSize,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolFreeSize,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolInUseSize,
                Is.EqualTo(recyclableMemoryStreamManager.LargeBufferMultiple)
            );

            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );

            Assert.That(
                recyclableMemoryStreamManager.LargePoolFreeSize,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.LargePoolInUseSize,
                Is.EqualTo(recyclableMemoryStreamManager.LargeBufferMultiple * (1 + 2))
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolFreeSize,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolInUseSize,
                Is.EqualTo(recyclableMemoryStreamManager.LargeBufferMultiple)
            );

            recyclableMemoryStream.Write(
                buffer,
                0,
                buffer.Length
            );

            Assert.That(
                recyclableMemoryStreamManager.LargePoolFreeSize,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.LargePoolInUseSize,
                Is.EqualTo(recyclableMemoryStreamManager.LargeBufferMultiple * (1 + 2 + 4))
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolFreeSize,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolInUseSize,
                Is.EqualTo(recyclableMemoryStreamManager.LargeBufferMultiple)
            );

            recyclableMemoryStream.Dispose();

            Assert.That(
                recyclableMemoryStreamManager.LargePoolFreeSize,
                Is.EqualTo(recyclableMemoryStreamManager.LargeBufferMultiple * (1 + 2 + 4))
            );
            Assert.That(
                recyclableMemoryStreamManager.LargePoolInUseSize,
                Is.EqualTo(0)
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolFreeSize,
                Is.EqualTo(recyclableMemoryStreamManager.LargeBufferMultiple)
            );
            Assert.That(
                recyclableMemoryStreamManager.SmallPoolInUseSize,
                Is.EqualTo(0)
            );
        }
    }

    [TestFixture]
    public sealed class RecyclableMemoryStreamTestsWithAggressiveBufferReleaseUsingExponentialLargeBuffer : BaseRecyclableMemoryStreamTestsUsingExponentialLargeBuffer
    {
        protected override bool AggressiveBufferRelease
        {
            get { return true; }
        }
    }
}
