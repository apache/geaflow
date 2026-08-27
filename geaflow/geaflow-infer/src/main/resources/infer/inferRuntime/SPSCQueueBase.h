/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef SPSC_QUEUE_BASE_H
#define SPSC_QUEUE_BASE_H

#include <atomic>
#include <stdexcept>
#include <thread>
#include <cstring>
#include <string>
#include <sstream>
#include <iostream>
#include <assert.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <chrono>

using byte = unsigned char;
using namespace std::chrono;

#define ATOMIC_INT32_SIZE sizeof(uint32_t)
#define CACHE_LINE_SIZE 64

class SPSCQueueBase
{
  protected:

    byte *alignedRaw_;

    byte *arrayBase_;

    std::atomic<int64_t> *const readAtomicPtr_;

    int64_t *const readPtr_;

    int64_t *const writeCachePtr_;

    std::atomic<int64_t> *const writeAtomicPtr_;

    int64_t *const writePtr_;

    int64_t *const readCachePtr_;

    std::atomic<int64_t> *const finishAtomicPtr_;

    int64_t *const barrierPtr_;

    int64_t *const lastClearBarrierPtr_;

    const int capacity_;

    const int mask_;

    const bool ipc_;

    const int mmapLen_;

  public:
    SPSCQueueBase(long alignedPtr, int64_t len) : alignedRaw_(reinterpret_cast<byte *>(alignedPtr)),
                                                   arrayBase_(reinterpret_cast<byte *>(alignedPtr + 4 * CACHE_LINE_SIZE)),

                                                   readAtomicPtr_(reinterpret_cast<std::atomic<int64_t> *>(alignedPtr)),
                                                   readPtr_(reinterpret_cast<int64_t *>(alignedPtr)),
                                                   writeCachePtr_(reinterpret_cast<int64_t *>(alignedPtr + 8)),

                                                   writeAtomicPtr_(reinterpret_cast<std::atomic<int64_t> *>(alignedPtr + 2 * CACHE_LINE_SIZE)),
                                                   writePtr_(reinterpret_cast<int64_t *>(alignedPtr + 2 * CACHE_LINE_SIZE)),
                                                   readCachePtr_(reinterpret_cast<int64_t *>(alignedPtr + 2 * CACHE_LINE_SIZE + 8)),

                                                   finishAtomicPtr_(reinterpret_cast<std::atomic<int64_t> *>(alignedPtr + 3 * CACHE_LINE_SIZE)),
                                                   barrierPtr_(reinterpret_cast<int64_t *>(alignedPtr + 3 * CACHE_LINE_SIZE + 8)),
                                                   lastClearBarrierPtr_(reinterpret_cast<int64_t *>(alignedPtr + 3 * CACHE_LINE_SIZE + 16)),
                                                   capacity_(*reinterpret_cast<int64_t *>(alignedPtr + CACHE_LINE_SIZE)),
                                                   mask_(capacity_ - 1),
                                                   ipc_(true),
                                                   mmapLen_(len)
    {
    }

    ~SPSCQueueBase() {}

    void close() {
        if(ipc_) {
            int rc = munmap(reinterpret_cast<void*>(alignedRaw_), mmapLen_);
            (void)rc;  // Suppress unused variable warning
            assert(rc==0);
        }
    }

  private:
    static inline int64_t getLong(const int64_t *ptr)
    {
        return ptr[0];
    }

    static inline void putLong(int64_t *ptr, int64_t value)
    {
        ptr[0] = value;
    }

    static inline int64_t getLongVolatile(std::atomic<int64_t> *ptr)
    {
        return ptr->load();
    }

    static inline void putOrderedLong(std::atomic<int64_t> *ptr, int64_t value)
    {
        ptr->store(value, std::memory_order_release);
    }

  protected:
    int64_t getReadPlain()
    {
        return getLong(readPtr_);
    }

    int64_t getRead()
    {
        return getLongVolatile(readAtomicPtr_);
    }

    void setRead(int64_t value)
    {
        putOrderedLong(readAtomicPtr_, value);
    }

    int64_t getWritePlain()
    {
        return getLong(writePtr_);
    }

    int64_t getWrite()
    {
        return getLongVolatile(writeAtomicPtr_);
    }

    void setWrite(int64_t value)
    {
        putOrderedLong(writeAtomicPtr_, value);
    }

    int64_t getReadCache()
    {
        return getLong(readCachePtr_);
    }

    int64_t getWriteCache()
    {
        return getLong(writeCachePtr_);
    }

    void setReadCache(int64_t value)
    {
        putLong(readCachePtr_, value);
    }

    void setWriteCache(int64_t value)
    {
        putLong(writeCachePtr_, value);
    }

    void setBarrier(int64_t barrier) {
        putLong(barrierPtr_, barrier);
    }

    int64_t getBarrier() {
        return getLong(barrierPtr_);
    }

    void setLastClearBarrier(int64_t clearBarrier) {
        putLong(lastClearBarrierPtr_, clearBarrier);
    }

    int64_t getLastClearBarrier() {
        return getLong(lastClearBarrierPtr_);
    }

    void markFinished()
    {
        putOrderedLong(finishAtomicPtr_, -1);
    }

    bool isFinished()
    {
        int64_t v = getLongVolatile(finishAtomicPtr_);
        return v != 0;
    }

    inline int64_t nextWrap(int64_t v)
    {
        if ((v & mask_) == 0)
        {
            return v + capacity_;
        }
        return (v + mask_) & ~mask_;
    }

    inline int64_t prevWrap(int64_t v)
    {
        return v & ~mask_;
    }

    void dump() {
        printf("alignedRaw=0x%llx, arrayBase=0x%llx, readPtr=0x%llx, writeCachePtr=0x%llx, writePtr=0x%llx, readCachePtr=0x%llx, finishPtr=0x%llx, capacity=%d, mmapLen=%d\n",
        reinterpret_cast<int64_t>(alignedRaw_),
        reinterpret_cast<int64_t>(arrayBase_),
        reinterpret_cast<int64_t>(readPtr_),
        reinterpret_cast<int64_t>(writeCachePtr_),
        reinterpret_cast<int64_t>(writePtr_),
        reinterpret_cast<int64_t>(readCachePtr_),
        reinterpret_cast<int64_t>(finishAtomicPtr_),
         capacity_, mmapLen_);
    }
};

#endif