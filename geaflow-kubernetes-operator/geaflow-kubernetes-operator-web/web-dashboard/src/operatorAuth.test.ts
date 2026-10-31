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

import { getOperatorToken, onOperatorAuthChanged, operatorApiUrl, operatorAuthHeaders, setOperatorToken } from './operatorAuth';

beforeEach(() => {
  setOperatorToken();
  window.history.replaceState({}, '', '/');
});

test('sends tokens only to the same operator API, including proxy prefixes', () => {
  setOperatorToken('test-token');
  expect(operatorAuthHeaders(operatorApiUrl('/api/jobs'))).toEqual({ Authorization: 'Bearer test-token' });
  for (const target of ['https://untrusted.invalid/api/jobs', '/api-other/jobs', '/scripts/test.js']) {
    expect(operatorAuthHeaders(target)).toEqual({});
  }
  window.history.replaceState({}, '', '/operator/index.html');
  expect(operatorApiUrl('/api/jobs')).toBe(`${window.location.origin}/operator/api/jobs`);
  expect(operatorAuthHeaders('/api/jobs')).toEqual({});
  expect(operatorAuthHeaders(operatorApiUrl('/api/jobs'))).toEqual({ Authorization: 'Bearer test-token' });
  window.history.replaceState({}, '', '/operator/');
  expect(operatorApiUrl('/api/jobs')).toBe(`${window.location.origin}/operator/api/jobs`);
});

test('clears credentials and notifies mounted consumers', () => {
  const listener = jest.fn();
  const unsubscribe = onOperatorAuthChanged(listener);
  setOperatorToken('test-token');
  expect(getOperatorToken()).toBe('test-token');
  setOperatorToken();
  expect(getOperatorToken()).toBeUndefined();
  expect(operatorAuthHeaders(operatorApiUrl('/api/jobs'))).toEqual({});
  expect(listener).toHaveBeenCalledTimes(2);
  unsubscribe();
  setOperatorToken();
  expect(listener).toHaveBeenCalledTimes(2);
});

test('rejects paths outside the operator API', () => {
  for (const path of ['https://untrusted.invalid/api/jobs', '//untrusted.invalid/api/jobs', '/health', '']) {
    expect(() => operatorApiUrl(path)).toThrow('Invalid operator API path');
  }
});
