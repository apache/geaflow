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

#ifndef SPSC_QUEUE_READ_H
#define SPSC_QUEUE_READ_H

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

class SPSCQueueRead : public SPSCQueueBase
{
  private:
    int toMove_;

    // 更新队列的读指针
    inline int64_t updateReadPtr()
    {
        int64_t currentRead = getReadPlain();
        if(toMove_ != 0) {
            currentRead += toMove_;
            // 设置当前读的指针
            setRead(currentRead);
            assert(toMove_ > 0);
        }
        toMove_ = 0;
        return currentRead;
    }
  public:
    SPSCQueueRead(const char* fileName, int64_t len): SPSCQueueBase(mmap(fileName, len), len), toMove_(0) {}

    virtual ~SPSCQueueRead() {}

    void close() {
        updateReadPtr();
        SPSCQueueBase::close();
    }

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

    virtual bool Next(const void **data, int *size) {
        bool isNext = Next(data, size, false, 0);
        return isNext;
    }

    virtual bool Next(const void **data, int *size, bool returnOnBarrier, size_t sizeToRead)
    {
        // 获取当前可读的指针
        int64_t currentRead = updateReadPtr();
        int64_t writeCache = getWriteCache();
        while (currentRead >= writeCache)
        {
            setWriteCache(getWrite());
            writeCache = getWriteCache();
            if (currentRead >= writeCache)
            {
                int64_t barrier = getBarrier();
                if (barrier != getLastClearBarrier() && currentRead >= barrier) {
                    setLastClearBarrier(barrier);
                    if (returnOnBarrier) {
                        return false;
                    }
                }
                if (isFinished())
                {
                    setWriteCache(getWrite());
                    writeCache = getWriteCache();
                    if (currentRead >= writeCache)
                    {
                        return false;
                    }
                    break;
                }

                if (currentRead == writeCache)
                {
                    if (sizeToRead == 4) {
                        break;
                    } else {
                        std::this_thread::yield();
                    }
                }
            }
        }
        long nextReadWrap = nextWrap(currentRead);
        int avail = writeCache > nextReadWrap ? (int)(nextReadWrap - currentRead) : (int)(writeCache - currentRead);
        size[0] = avail;
        data[0] = reinterpret_cast<const void *>(arrayBase_ + (currentRead & mask_));
        toMove_ = avail;
        return true;
    }


    virtual void BackUp(int count)
    {
        toMove_ -= count;
        updateReadPtr();
    }


    virtual bool Skip(int count)
    {
        int remain = count;
        while (remain > 0)
        {
            const void *data;
            int size;
            if (!Next(&data, &size))
            {
                return false;
            }
            if (size > remain)
            {
                BackUp(size - remain);
                remain = 0;
            }
            else
            {
                remain -= size;
            }
        }
        return true;
    }

    int readBytes(void *buf, size_t sizeToRead) {
        int len = readBytes(buf, sizeToRead, false);
        return len;
    }

    int readBytes(void *buf, size_t sizeToRead, bool returnOnBarrier)
    {
        const void *data;
        int size;
        int remain = sizeToRead;
        char *p = reinterpret_cast<char *>(buf);

        while (Next(&data, &size, returnOnBarrier, sizeToRead))
        {
            if (size == 0) {
                return size;
            }
            const char *q = reinterpret_cast<const char *>(data);
            int s = remain > size ? size : remain;
            std::memcpy(p + sizeToRead - remain, q, s);
            if (size >= remain)
            {
                if (size > remain)
                {
                    BackUp(size - remain);
                }
                return sizeToRead;
            }
            remain -= s;
        }
        return sizeToRead - remain;
    }

    uint32_t readInt()
    {
        uint32_t result;
        readBytes(&result, sizeof(uint32_t));
        return result;
    }
};

#endif


