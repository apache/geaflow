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

import {Request, Response} from 'express';
import {parse} from 'url';

function getFlameGraphContent(req: Request, res: Response) {

  let content = "<!DOCTYPE html>\n" +
    "<html lang='en'>\n" +
    "<head>\n" +
    "<meta charset='utf-8'>\n" +
    "<style>\n" +
    "\tbody {margin: 0; padding: 10px; background-color: #ffffff}\n" +
    "\th1 {margin: 5px 0 0 0; font-size: 18px; font-weight: normal; text-align: center}\n" +
    "\theader {margin: -24px 0 5px 0; line-height: 24px}\n" +
    "\tbutton {font: 12px sans-serif; cursor: pointer}\n" +
    "\tp {margin: 5px 0 5px 0}\n" +
    "\ta {color: #0366d6}\n" +
    "\t#hl {position: absolute; display: none; overflow: hidden; white-space: nowrap; pointer-events: none; background-color: #ffffe0; outline: 1px solid #ffc000; height: 15px}\n" +
    "\t#hl span {padding: 0 3px 0 3px}\n" +
    "\t#status {overflow: hidden; white-space: nowrap}\n" +
    "\t#match {overflow: hidden; white-space: nowrap; display: none; float: right; text-align: right}\n" +
    "\t#reset {cursor: pointer}\n" +
    "\t#canvas {width: 100%; height: 512px}\n" +
    "</style>\n" +
    "</head>\n" +
    "<body style='font: 12px Verdana, sans-serif'>\n" +
    "<h1>CPU profile</h1>\n" +
    "<header style='text-align: left'><button id='reverse' title='Reverse'>&#x1f53b;</button>&nbsp;&nbsp;<button id='search' title='Search'>&#x1f50d;</button></header>\n" +
    "<header style='text-align: right'>Produced by <a href='https://github.com/jvm-profiling-tools/async-profiler'>async-profiler</a></header>\n" +
    "<canvas id='canvas'></canvas>\n" +
    "<div id='hl'><span></span></div>\n" +
    "<p id='match'>Matched: <span id='matchval'></span> <span id='reset' title='Clear'>&#x274c;</span></p>\n" +
    "<p id='status'>&nbsp;</p>\n" +
    "<script>\n" +
    "\t'use strict';\n" +
    "\tvar root, rootLevel, px, pattern;\n" +
    "\tvar reverse = false;\n" +
    "\tconst levels = Array(32);\n" +
    "\tfor (let h = 0; h < levels.length; h++) {\n" +
    "\t\tlevels[h] = [];\n" +
    "\t}\n" +
    "\n" +
    "\tconst canvas = document.getElementById('canvas');\n" +
    "\tconst c = canvas.getContext('2d');\n" +
    "\tconst hl = document.getElementById('hl');\n" +
    "\tconst status = document.getElementById('status');\n" +
    "\n" +
    "\tconst canvasWidth = canvas.offsetWidth;\n" +
    "\tconst canvasHeight = canvas.offsetHeight;\n" +
    "\tcanvas.style.width = canvasWidth + 'px';\n" +
    "\tcanvas.width = canvasWidth * (devicePixelRatio || 1);\n" +
    "\tcanvas.height = canvasHeight * (devicePixelRatio || 1);\n" +
    "\tif (devicePixelRatio) c.scale(devicePixelRatio, devicePixelRatio);\n" +
    "\tc.font = document.body.style.font;\n" +
    "\n" +
    "\tconst palette = [\n" +
    "\t\t[0xb2e1b2, 20, 20, 20],\n" +
    "\t\t[0x50e150, 30, 30, 30],\n" +
    "\t\t[0x50cccc, 30, 30, 30],\n" +
    "\t\t[0xe15a5a, 30, 40, 40],\n" +
    "\t\t[0xc8c83c, 30, 30, 10],\n" +
    "\t\t[0xe17d00, 30, 30,  0],\n" +
    "\t\t[0xcce880, 20, 20, 20],\n" +
    "\t];\n" +
    "\n" +
    "\tfunction getColor(p) {\n" +
    "\t\tconst v = Math.random();\n" +
    "\t\treturn '#' + (p[0] + ((p[1] * v) << 16 | (p[2] * v) << 8 | (p[3] * v))).toString(16);\n" +
    "\t}\n" +
    "\n" +
    "\tfunction f(level, left, width, type, title, inln, c1, int) {\n" +
    "\t\tlevels[level].push({left: left, width: width, color: getColor(palette[type]), title: title,\n" +
    "\t\t\tdetails: (int ? ', int=' + int : '') + (c1 ? ', c1=' + c1 : '') + (inln ? ', inln=' + inln : '')\n" +
    "\t\t});\n" +
    "\t}\n" +
    "\n" +
    "\tfunction samples(n) {\n" +
    "\t\treturn n === 1 ? '1 sample' : n.toString().replace(/\\B(?=(\\d{3})+(?!\\d))/g, ',') + ' samples';\n" +
    "\t}\n" +
    "\n" +
    "\tfunction pct(a, b) {\n" +
    "\t\treturn a >= b ? '100' : (100 * a / b).toFixed(2);\n" +
    "\t}\n" +
    "\n" +
    "\tfunction findFrame(frames, x) {\n" +
    "\t\tlet left = 0;\n" +
    "\t\tlet right = frames.length - 1;\n" +
    "\n" +
    "\t\twhile (left <= right) {\n" +
    "\t\t\tconst mid = (left + right) >>> 1;\n" +
    "\t\t\tconst f = frames[mid];\n" +
    "\n" +
    "\t\t\tif (f.left > x) {\n" +
    "\t\t\t\tright = mid - 1;\n" +
    "\t\t\t} else if (f.left + f.width <= x) {\n" +
    "\t\t\t\tleft = mid + 1;\n" +
    "\t\t\t} else {\n" +
    "\t\t\t\treturn f;\n" +
    "\t\t\t}\n" +
    "\t\t}\n" +
    "\n" +
    "\t\tif (frames[left] && (frames[left].left - x) * px < 0.5) return frames[left];\n" +
    "\t\tif (frames[right] && (x - (frames[right].left + frames[right].width)) * px < 0.5) return frames[right];\n" +
    "\n" +
    "\t\treturn null;\n" +
    "\t}\n" +
    "\n" +
    "\tfunction search(r) {\n" +
    "\t\tif (r === true && (r = prompt('Enter regexp to search:', '')) === null) {\n" +
    "\t\t\treturn;\n" +
    "\t\t}\n" +
    "\n" +
    "\t\tpattern = r ? RegExp(r) : undefined;\n" +
    "\t\tconst matched = render(root, rootLevel);\n" +
    "\t\tdocument.getElementById('matchval').textContent = pct(matched, root.width) + '%';\n" +
    "\t\tdocument.getElementById('match').style.display = r ? 'inherit' : 'none';\n" +
    "\t}\n" +
    "\n" +
    "\tfunction render(newRoot, newLevel) {\n" +
    "\t\tif (root) {\n" +
    "\t\t\tc.fillStyle = '#ffffff';\n" +
    "\t\t\tc.fillRect(0, 0, canvasWidth, canvasHeight);\n" +
    "\t\t}\n" +
    "\n" +
    "\t\troot = newRoot || levels[0][0];\n" +
    "\t\trootLevel = newLevel || 0;\n" +
    "\t\tpx = canvasWidth / root.width;\n" +
    "\n" +
    "\t\tconst x0 = root.left;\n" +
    "\t\tconst x1 = x0 + root.width;\n" +
    "\t\tconst marked = [];\n" +
    "\n" +
    "\t\tfunction mark(f) {\n" +
    "\t\t\treturn marked[f.left] >= f.width || (marked[f.left] = f.width);\n" +
    "\t\t}\n" +
    "\n" +
    "\t\tfunction totalMarked() {\n" +
    "\t\t\tlet total = 0;\n" +
    "\t\t\tlet left = 0;\n" +
    "\t\t\tObject.keys(marked).sort(function(a, b) { return a - b; }).forEach(function(x) {\n" +
    "\t\t\t\tif (+x >= left) {\n" +
    "\t\t\t\t\ttotal += marked[x];\n" +
    "\t\t\t\t\tleft = +x + marked[x];\n" +
    "\t\t\t\t}\n" +
    "\t\t\t});\n" +
    "\t\t\treturn total;\n" +
    "\t\t}\n" +
    "\n" +
    "\t\tfunction drawFrame(f, y, alpha) {\n" +
    "\t\t\tif (f.left < x1 && f.left + f.width > x0) {\n" +
    "\t\t\t\tc.fillStyle = pattern && f.title.match(pattern) && mark(f) ? '#ee00ee' : f.color;\n" +
    "\t\t\t\tc.fillRect((f.left - x0) * px, y, f.width * px, 15);\n" +
    "\n" +
    "\t\t\t\tif (f.width * px >= 21) {\n" +
    "\t\t\t\t\tconst chars = Math.floor(f.width * px / 7);\n" +
    "\t\t\t\t\tconst title = f.title.length <= chars ? f.title : f.title.substring(0, chars - 2) + '..';\n" +
    "\t\t\t\t\tc.fillStyle = '#000000';\n" +
    "\t\t\t\t\tc.fillText(title, Math.max(f.left - x0, 0) * px + 3, y + 12, f.width * px - 6);\n" +
    "\t\t\t\t}\n" +
    "\n" +
    "\t\t\t\tif (alpha) {\n" +
    "\t\t\t\t\tc.fillStyle = 'rgba(255, 255, 255, 0.5)';\n" +
    "\t\t\t\t\tc.fillRect((f.left - x0) * px, y, f.width * px, 15);\n" +
    "\t\t\t\t}\n" +
    "\t\t\t}\n" +
    "\t\t}\n" +
    "\n" +
    "\t\tfor (let h = 0; h < levels.length; h++) {\n" +
    "\t\t\tconst y = reverse ? h * 16 : canvasHeight - (h + 1) * 16;\n" +
    "\t\t\tconst frames = levels[h];\n" +
    "\t\t\tfor (let i = 0; i < frames.length; i++) {\n" +
    "\t\t\t\tdrawFrame(frames[i], y, h < rootLevel);\n" +
    "\t\t\t}\n" +
    "\t\t}\n" +
    "\n" +
    "\t\treturn totalMarked();\n" +
    "\t}\n" +
    "\n" +
    "\tcanvas.onmousemove = function() {\n" +
    "\t\tconst h = Math.floor((reverse ? event.offsetY : (canvasHeight - event.offsetY)) / 16);\n" +
    "\t\tif (h >= 0 && h < levels.length) {\n" +
    "\t\t\tconst f = findFrame(levels[h], event.offsetX / px + root.left);\n" +
    "\t\t\tif (f) {\n" +
    "\t\t\t\thl.style.left = (Math.max(f.left - root.left, 0) * px + canvas.offsetLeft) + 'px';\n" +
    "\t\t\t\thl.style.width = (Math.min(f.width, root.width) * px) + 'px';\n" +
    "\t\t\t\thl.style.top = ((reverse ? h * 16 : canvasHeight - (h + 1) * 16) + canvas.offsetTop) + 'px';\n" +
    "\t\t\t\thl.firstChild.textContent = f.title;\n" +
    "\t\t\t\thl.style.display = 'block';\n" +
    "\t\t\t\tcanvas.title = f.title + '\\n(' + samples(f.width) + f.details + ', ' + pct(f.width, levels[0][0].width) + '%)';\n" +
    "\t\t\t\tcanvas.style.cursor = 'pointer';\n" +
    "\t\t\t\tcanvas.onclick = function() {\n" +
    "\t\t\t\t\tif (f != root) {\n" +
    "\t\t\t\t\t\trender(f, h);\n" +
    "\t\t\t\t\t\tcanvas.onmousemove();\n" +
    "\t\t\t\t\t}\n" +
    "\t\t\t\t};\n" +
    "\t\t\t\tstatus.textContent = 'Function: ' + canvas.title;\n" +
    "\t\t\t\treturn;\n" +
    "\t\t\t}\n" +
    "\t\t}\n" +
    "\t\tcanvas.onmouseout();\n" +
    "\t}\n" +
    "\n" +
    "\tcanvas.onmouseout = function() {\n" +
    "\t\thl.style.display = 'none';\n" +
    "\t\tstatus.textContent = '\xa0';\n" +
    "\t\tcanvas.title = '';\n" +
    "\t\tcanvas.style.cursor = '';\n" +
    "\t\tcanvas.onclick = '';\n" +
    "\t}\n" +
    "\n" +
    "\tdocument.getElementById('reverse').onclick = function() {\n" +
    "\t\treverse = !reverse;\n" +
    "\t\trender();\n" +
    "\t}\n" +
    "\n" +
    "\tdocument.getElementById('search').onclick = function() {\n" +
    "\t\tsearch(true);\n" +
    "\t}\n" +
    "\n" +
    "\tdocument.getElementById('reset').onclick = function() {\n" +
    "\t\tsearch(false);\n" +
    "\t}\n" +
    "\n" +
    "\twindow.onkeydown = function() {\n" +
    "\t\tif (event.ctrlKey && event.keyCode === 70) {\n" +
    "\t\t\tevent.preventDefault();\n" +
    "\t\t\tsearch(true);\n" +
    "\t\t} else if (event.keyCode === 27) {\n" +
    "\t\t\tsearch(false);\n" +
    "\t\t}\n" +
    "\t}\n" +
    "\n" +
    "f(0,0,18,3,'all')\n" +
    "f(1,0,5,3,'[deoptimization]')\n" +
    "f(2,0,4,4,'Deoptimization::fetch_unroll_info(JavaThread*)')\n" +
    "f(3,0,4,4,'Deoptimization::fetch_unroll_info_helper(JavaThread*)')\n" +
    "f(4,0,1,4,'CHeapObj<(MemoryType)6>::operator new(unsigned long)')\n" +
    "f(5,0,1,4,'CHeapObj<(MemoryType)6>::operator new(unsigned long, NativeCallStack const&)')\n" +
    "f(6,0,1,4,'os::malloc(unsigned long, MemoryType, NativeCallStack const&)')\n" +
    "f(7,0,1,3,'nanov2_malloc')\n" +
    "f(4,1,2,4,'CHeapObj<(MemoryType)7>::operator new(unsigned long)')\n" +
    "f(5,1,2,4,'CHeapObj<(MemoryType)7>::operator new(unsigned long, NativeCallStack const&)')\n" +
    "f(6,1,1,3,'_malloc_zone_malloc')\n" +
    "f(6,2,1,4,'os::malloc(unsigned long, MemoryType, NativeCallStack const&)')\n" +
    "f(7,2,1,3,'nanov2_malloc')\n" +
    "f(4,3,1,4,'Deoptimization::create_vframeArray(JavaThread*, frame, RegisterMap*, GrowableArray<compiledVFrame*>*, bool)')\n" +
    "f(5,3,1,4,'vframeArray::allocate(JavaThread*, int, GrowableArray<compiledVFrame*>*, RegisterMap*, frame, frame, frame, bool)')\n" +
    "f(6,3,1,3,'szone_malloc')\n" +
    "f(2,4,1,4,'Deoptimization::unpack_frames(JavaThread*, int)')\n" +
    "f(3,4,1,4,'vframeArray::unpack_to_stack(frame&, int, int)')\n" +
    "f(4,4,1,4,'AbstractCompiler::is_c2()')\n" +
    "f(1,5,10,1,'java/lang/Thread.run')\n" +
    "f(2,5,8,1,'com/lmax/disruptor/BatchEventProcessor.run')\n" +
    "f(3,5,8,1,'com/lmax/disruptor/BatchEventProcessor.processEvents')\n" +
    "f(4,5,8,1,'com/lmax/disruptor/ProcessingSequenceBarrier.waitFor')\n" +
    "f(5,5,8,1,'com/lmax/disruptor/TimeoutBlockingWaitStrategy.waitFor',0,0,2)\n" +
    "f(6,6,1,4,'InterpreterRuntime::method(JavaThread*)')\n" +
    "f(6,7,6,1,'java/util/concurrent/locks/AbstractQueuedSynchronizer$ConditionObject.awaitNanos',0,1,0)\n" +
    "f(7,7,1,2,'java/util/concurrent/locks/AbstractQueuedSynchronizer$ConditionObject.checkInterruptWhileWaiting',1,0,0)\n" +
    "f(8,7,1,2,'java/lang/Thread.interrupted',1,0,0)\n" +
    "f(7,8,5,1,'java/util/concurrent/locks/LockSupport.parkNanos')\n" +
    "f(8,8,5,1,'sun/misc/Unsafe.park')\n" +
    "f(9,9,4,3,'Unsafe_Park')\n" +
    "f(10,9,4,4,'Parker::park(bool, long)')\n" +
    "f(11,9,1,3,'__gettimeofday')\n" +
    "f(11,10,3,3,'__psynch_cvwait')\n" +
    "f(2,13,1,1,'com/taobao/remoting/impl/TimeoutRespScan.run')\n" +
    "f(3,13,1,1,'java/lang/Thread.sleep')\n" +
    "f(4,13,1,3,'JVM_Sleep')\n" +
    "f(5,13,1,4,'os::sleep(Thread*, long, bool)')\n" +
    "f(6,13,1,4,'os::PlatformEvent::park(long)')\n" +
    "f(7,13,1,3,'__psynch_cvwait')\n" +
    "f(2,14,1,1,'java/util/concurrent/ThreadPoolExecutor$Worker.run')\n" +
    "f(3,14,1,1,'java/util/concurrent/ThreadPoolExecutor.runWorker')\n" +
    "f(4,14,1,1,'alipay/middleware/org/apache/mina/util/NamePreservingRunnable.run')\n" +
    "f(5,14,1,1,'alipay/middleware/org/apache/mina/transport/socket/nio/SocketIoProcessor$Worker.run')\n" +
    "f(6,14,1,1,'alipay/middleware/org/apache/mina/transport/socket/nio/SocketIoProcessor.access$400')\n" +
    "f(7,14,1,1,'alipay/middleware/org/apache/mina/transport/socket/nio/SocketIoProcessor.process')\n" +
    "f(8,14,1,1,'alipay/middleware/org/apache/mina/transport/socket/nio/SocketIoProcessor.read')\n" +
    "f(9,14,1,1,'alipay/middleware/org/apache/mina/common/support/AbstractIoFilterChain.fireMessageReceived')\n" +
    "f(10,14,1,1,'alipay/middleware/org/apache/mina/common/support/AbstractIoFilterChain.callNextMessageReceived')\n" +
    "f(11,14,1,1,'alipay/middleware/org/apache/mina/common/support/AbstractIoFilterChain$HeadFilter.messageReceived')\n" +
    "f(12,14,1,1,'alipay/middleware/org/apache/mina/common/support/AbstractIoFilterChain$EntryImpl$1.messageReceived')\n" +
    "f(13,14,1,1,'alipay/middleware/org/apache/mina/common/support/AbstractIoFilterChain.access$1100')\n" +
    "f(14,14,1,1,'alipay/middleware/org/apache/mina/common/support/AbstractIoFilterChain.callNextMessageReceived')\n" +
    "f(15,14,1,1,'alipay/middleware/org/apache/mina/filter/codec/ProtocolCodecFilter.messageReceived')\n" +
    "f(16,14,1,1,'com/taobao/remoting/serialize/impl/RemotingProtocolDecoder.decode')\n" +
    "f(17,14,1,1,'com/taobao/remoting/serialize/impl/RemotingProtocolDecoder.doDecode')\n" +
    "f(18,14,1,1,'com/taobao/remoting/serialize/impl/ConnResponseSerialization.deserialize')\n" +
    "f(19,14,1,1,'com/taobao/remoting/serialize/DefaultSerialization.deserialize')\n" +
    "f(20,14,1,1,'java/io/ObjectInputStream.readObject')\n" +
    "f(21,14,1,1,'java/io/ObjectInputStream.readObject')\n" +
    "f(22,14,1,1,'java/io/ObjectInputStream.readObject0')\n" +
    "f(23,14,1,1,'java/io/ObjectInputStream.readOrdinaryObject')\n" +
    "f(24,14,1,1,'java/io/ObjectInputStream.readClassDesc')\n" +
    "f(25,14,1,1,'java/io/ObjectInputStream.readNonProxyDesc')\n" +
    "f(26,14,1,1,'java/io/ObjectInputStream.readClassDescriptor')\n" +
    "f(27,14,1,1,'java/io/ObjectStreamClass.readNonProxy')\n" +
    "f(28,14,1,1,'java/io/ObjectInputStream.readTypeString')\n" +
    "f(29,14,1,1,'java/io/ObjectInputStream.readString')\n" +
    "f(30,14,1,1,'java/io/ObjectInputStream$BlockDataInputStream.readUTF')\n" +
    "f(31,14,1,6,'java/io/ObjectInputStream$BlockDataInputStream.readUTFBody',0,1,0)\n" +
    "f(1,15,3,3,'thread_start')\n" +
    "f(2,15,3,3,'_pthread_start')\n" +
    "f(3,15,3,3,'java_start(Thread*)')\n" +
    "f(4,15,1,4,'VMThread::run()')\n" +
    "f(5,15,1,4,'VMThread::loop()')\n" +
    "f(6,15,1,4,'Monitor::wait(bool, long, bool)')\n" +
    "f(7,15,1,4,'Monitor::IWait(Thread*, long)')\n" +
    "f(8,15,1,4,'Thread::muxAcquire(long volatile*, char const*)')\n" +
    "f(4,16,2,4,'WatcherThread::run()')\n" +
    "f(5,16,2,4,'WatcherThread::sleep() const')\n" +
    "f(6,16,2,4,'Monitor::wait(bool, long, bool)')\n" +
    "f(7,16,2,4,'Monitor::IWait(Thread*, long)')\n" +
    "f(8,16,2,4,'os::PlatformEvent::park(long)')\n" +
    "f(9,16,2,3,'__psynch_cvwait')\n" +
    "\n" +
    "search();\n" +
    "</script></body></html>\n";

  let result = {
    data: content,
    success: true
  }
  return res.json(result);
}

export default {
  'GET /proxy/:agentUrl/rest/flame-graphs/content': getFlameGraphContent,
};
