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

#include "SPSCQueueBase.h"

#ifndef SPSC_QUEUE_WRITE_H
#define SPSC_QUEUE_WRITE_H

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

using namespace std::chrono;

#define ATOMIC_INT32_SIZE sizeof(uint32_t)
#define CACHE_LINE_SIZE 64

class SPSCQueueWrite : public SPSCQueueBase
{
  private:
    int toMove_;

    inline int64_t updateWritePtr() {
        int64_t currentWrite = getWritePlain();
        if(toMove_ != 0) {
            currentWrite += toMove_;
            setWrite(currentWrite);
            assert(toMove_ > 0);
        }
        toMove_ = 0;
        return currentWrite;
    }
  public:
    SPSCQueueWrite(const char* fileName, int64_t len): SPSCQueueBase(mmap(fileName, len), len), toMove_(0) {}

    virtual ~SPSCQueueWrite() {}

    static int64_t mmap(const char* fileName, int64_t len)
    {
        int fd = open(fileName, O_RDWR);
        assert(fd!=-1);
        void* mmappedData = ::mmap(NULL, len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        assert(mmappedData != MAP_FAILED);
        ::close(fd);
        printf("MMap %s file to address 0x%llx with length %lld.\n", fileName, reinterpret_cast<int64_t>(mmappedData), len);
        printf("MMap capacity %lld.\n", *reinterpret_cast<int64_t *>(reinterpret_cast<int64_t>(mmappedData) + CACHE_LINE_SIZE));
        return reinterpret_cast<int64_t>(mmappedData);
    }
    virtual bool Next(void **data, int *size)
    {
        int capacity = capacity_;
        int64_t currentWrite = updateWritePtr();
        int64_t readWatermark = currentWrite - capacity;
        while (getReadCache() <= readWatermark)
        {
            setReadCache(getRead());
            if (getReadCache() <= readWatermark)
            {
                std::this_thread::yield();
            }
        }

        int64_t prevWriteWrap = prevWrap(currentWrite);
        int64_t nextWriteWrap = nextWrap(currentWrite);

        int bytesToWrite;
        if (getReadCache() >= prevWriteWrap)
        {

            bytesToWrite = (int)(nextWriteWrap - currentWrite);
        }
        else
        {
            bytesToWrite = capacity - (currentWrite - getReadCache());
        }
        size[0] = bytesToWrite;
        data[0] = reinterpret_cast<void *>(arrayBase_ + (currentWrite & mask_));
        toMove_ = bytesToWrite;


        return true;
    }

    virtual void BackUp(int count)
    {
        toMove_ -= count;
        updateWritePtr();
    }

    void close()
    {
        updateWritePtr();
        markFinished();
    }

    void writeBytes(const char *val, size_t sizeToWrite)
    {
        void *data;
        int size;

        int total = sizeToWrite;
        int remain = total;
        const char *p = val;

        while (Next(&data, &size))
        {
            char *q = reinterpret_cast<char *>(data);
            int s = remain > size ? size : remain;
            std::memcpy(q, p + total - remain, s);
            if (size >= remain)
            {
                if (size > remain)
                {
                    BackUp(size - remain);
                }
                return;
            }
            remain -= s;
        }
    }

    void writeInt(uint32_t val)
    {
        const char *p = reinterpret_cast<const char *>(&val);
        writeBytes(p, sizeof(val));
    }

    void writeString(const char *val)
    {
        void *data;
        int size;
        const char *p = val;
        bool finishWrite = false;
        while (Next(&data, &size))
        {
            char *q = reinterpret_cast<char *>(data);
            int i = 0;
            for (i = 0; i < size; i++)
            {
                q[i] = p[i];
                if (p[i] == 0)
                {
                    finishWrite = true;
                    break;
                }
            }
            if (finishWrite)
            {
                BackUp(size - i - 1);
                return;
            }
            else
            {
                p += size;
            }
        }
    }
};

#endif


