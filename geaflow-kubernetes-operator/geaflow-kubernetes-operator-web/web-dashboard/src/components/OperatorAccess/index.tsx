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

import React, { useEffect, useState } from 'react';
import { Alert, Button, Card, Form, Input } from 'antd';
import { getOperatorToken, onOperatorAuthChanged, operatorApiUrl, setOperatorToken } from '@/operatorAuth';

const OperatorAccess: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [authenticated, setAuthenticated] = useState(!!getOperatorToken());
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [form] = Form.useForm();

  useEffect(() => onOperatorAuthChanged(() => setAuthenticated(!!getOperatorToken())), []);

  const connect = async ({ token }: { token: string }) => {
    setLoading(true);
    setError('');
    try {
      const response = await fetch(operatorApiUrl('/api/jobs'), {
        headers: { Authorization: `Bearer ${token}` },
        credentials: 'omit',
        redirect: 'error',
        cache: 'no-store',
      });
      if (!response.ok) {
        throw new Error('Access failed. Check the token and operator connection.');
      }
      const body = await response.json();
      if (!body.success) {
        throw new Error('The operator could not verify access.');
      }
      form.resetFields();
      setOperatorToken(token);
    } catch {
      setError('Access failed. Check the token and operator connection.');
    } finally {
      setLoading(false);
    }
  };

  if (authenticated) {
    return <>
      <Button onClick={() => setOperatorToken()} style={{ marginBottom: 16 }}>Disconnect</Button>
      {children}
    </>;
  }

  return <Card title="Operator access" style={{ maxWidth: 480, margin: '48px auto' }}>
    <p>Enter your administrator or read-only token. Access is cleared when this page reloads.</p>
    {error && <Alert type="error" message={error} style={{ marginBottom: 16 }} />}
    <Form form={form} onFinish={connect} layout="vertical">
      <Form.Item name="token" label="Access token" rules={[{ required: true, message: 'Enter an access token.' }]}>
        <Input.Password autoComplete="off" />
      </Form.Item>
      <Button type="primary" htmlType="submit" loading={loading}>Connect</Button>
    </Form>
  </Card>;
};

export default OperatorAccess;
