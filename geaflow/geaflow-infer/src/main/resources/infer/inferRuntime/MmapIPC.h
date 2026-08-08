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

#ifndef MMAP_IPC_H
#define MMAP_IPC_H
#include "SPSCQueueRead.h"
#include "SPSCQueueBase.h"
#include "SPSCQueueWrite.h"
#include <chrono>
#include <vector>
#include <Python.h>

using namespace std::chrono;
using namespace std;

class MmapIPC{

public:
    MmapIPC(const std::string &mmap_read_key, const std::string &mmap_write_key) : mmap_read_key(mmap_read_key),
                                                                                      mmap_write_key(mmap_write_key) {
        long buf_r;
        std::string readMmap;
        ParseQueuePath(mmap_read_key, readMmap, &buf_r);
        reader = new SPSCQueueRead(readMmap.c_str(), buf_r);
        long buf_w;
        std::string writeMmap;
        ParseQueuePath(mmap_write_key, writeMmap, &buf_w);
        writer = new SPSCQueueWrite(writeMmap.c_str(), buf_w);
        readBuffer = new char[8*1024*1024];
        readBufferPtr = reinterpret_cast<uint8_t *>(readBuffer);
    }

    bool writeBytes(char* buf, size_t len){
        writer->writeBytes(buf, len);
        return true;
    }

    int readBytes(size_t len){
        int readSize = reader->readBytes(readBuffer, len);
        return readSize;
    }

    // 返回指向 readBuffer 的指针
    inline uint8_t *getReadBufferPtr() const {return readBufferPtr;}

    virtual ~MmapIPC() {
        if(nullptr != reader){
            reader->close();
            delete(reader);
            reader = nullptr;
        }
        if(nullptr != writer){
            writer->close();
            delete(writer);
            writer = nullptr;
        }
        delete[] readBuffer;
        readBuffer = nullptr;
        readBufferPtr = nullptr;
    }

    bool ParseQueuePath(const std::string &mmap_key, std::string& mmapName, long* buf) {
        if (!buf) {
            std::cerr << "buf can not be null." << std::endl;
            return false;
        }
        const char* prefix = "queue://";
        if(mmap_key.find(prefix)!=0) {
            std::cerr << "queue path doesn't start with 'queue://': " << mmap_key << std::endl;
            return false;
        }
        std::string fName = mmap_key.substr(strlen(prefix));
        std::vector<std::string> tokens;
        std::string token;
        std::istringstream tokenStream(fName);
        while (std::getline(tokenStream, token, ':'))
        {
            tokens.push_back(token);
        }
        if(tokens.size()==1) {
            buf[0] = atol(tokens[0].c_str());
        }else if(tokens.size()==2) {
            mmapName = tokens[0];
            buf[0] = atol(tokens[1].c_str());
        }else {
            std::cerr << "Invalid queue construction." << mmap_key << std::endl;
            std::cerr.flush();
            return false;
        }
        std::cout << "mmapName: " << mmapName << std::endl;
        std::cout << "buf:" << *buf << std::endl;
        std::cout.flush();
        return true;
    }

private:
    SPSCQueueRead* reader;
    SPSCQueueWrite* writer;
    std::string mmap_read_key;
    std::string mmap_write_key;
    char* readBuffer;
    uint8_t* readBufferPtr;
};
#endif //MMAP_IPC_H
