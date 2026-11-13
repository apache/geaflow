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

// Credentials live only in this tab's memory. Never persist them in browser storage or URLs.
let token: string | undefined;
const authChanged = 'geaflow-operator-auth-changed';

export function getOperatorToken() {
  return token;
}

export function setOperatorToken(value?: string) {
  token = value;
  window.dispatchEvent(new Event(authChanged));
}

export function onOperatorAuthChanged(listener: () => void) {
  window.addEventListener(authChanged, listener);
  return () => window.removeEventListener(authChanged, listener);
}

export function operatorApiUrl(path: string) {
  if (!path.startsWith('/api/')) {
    throw new Error('Invalid operator API path');
  }
  return new URL(`.${path}`, new URL('.', window.location.href)).href;
}

export function operatorAuthHeaders(url: string): Record<string, string> {
  const apiBase = operatorApiUrl('/api/');
  const target = new URL(url, window.location.href).href;
  return token && target.startsWith(apiBase) ? { Authorization: `Bearer ${token}` } : {};
}
