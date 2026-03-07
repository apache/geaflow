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

/**
 * Run "node server.js" command to start the webpage
 * after "npm run build" command.
 * @type {e | (() => Express)}
 */
const express = require('express')
const path = require('node:path')
const proxy = require("http-proxy-middleware").createProxyMiddleware
const app = express()
const port = 8002
app.use(express.static('resources/dist'))
app.use('/rest', proxy({ target: 'http://localhost:8090/', changeOrigin: true }));
app.use('/proxy', proxy({ target: 'http://localhost:8090/', changeOrigin: true }));
app.get('/*', function (req, res) {
  res.sendFile(path.join(__dirname, 'resources', 'dist', 'index.html'));
});
app.listen(port, () => console.log(`Geaflow-dashboard web server starts with port ${port}!`))
