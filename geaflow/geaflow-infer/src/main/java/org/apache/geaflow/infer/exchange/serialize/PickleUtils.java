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

package org.apache.geaflow.infer.exchange.serialize;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;

public class PickleUtils {

    public static String readLine(InputStream input) throws IOException {
        return readLine(input, false);
    }

    public static String readLine(InputStream input, boolean includeLF) throws IOException {
        StringBuilder sb = new StringBuilder();
        while (true) {
            int c = input.read();
            if (c == -1) {
                if (sb.length() == 0) {
                    throw new IOException("premature end of file");
                }
                break;
            }
            if (c != '\n' || includeLF) {
                sb.append((char) c);
            }
            if (c == '\n') {
                break;
            }
        }
        return sb.toString();
    }

    public static short readByte(InputStream input) throws IOException {
        int b = input.read();
        return (short) b;
    }

    public static byte[] readBytes(InputStream input, int n) throws IOException {
        byte[] buffer = new byte[n];
        readBytesInto(input, buffer, 0, n);
        return buffer;
    }

    public static byte[] readBytes(InputStream input, long n) throws IOException {
        if (n > Integer.MAX_VALUE) {
            throw new PickleException("pickle too large, can't read more than maxint");
        }
        return readBytes(input, (int) n);
    }

    public static void readBytesInto(InputStream input, byte[] buffer, int offset, int length) throws IOException {
        while (length > 0) {
            int read = input.read(buffer, offset, length);
            if (read == -1) {
                throw new IOException("expected more bytes in input stream");
            }
            offset += read;
            length -= read;
        }
    }

    public static int bytes2Integer(byte[] bytes) {
        return bytes2Integer(bytes, 0, bytes.length);
    }

    public static int bytes2Integer(byte[] bytes, int offset, int size) {
        if (size == 2) {
            int i = bytes[1 + offset] & 0xff;
            i <<= 8;
            i |= bytes[offset] & 0xff;
            return i;
        } else if (size == 4) {
            int i = bytes[3 + offset];
            i <<= 8;
            i |= bytes[2 + offset] & 0xff;
            i <<= 8;
            i |= bytes[1 + offset] & 0xff;
            i <<= 8;
            i |= bytes[offset] & 0xff;
            return i;
        } else {
            throw new PickleException("invalid amount of bytes to convert to int: " + size);
        }
    }


    public static long bytes2Long(byte[] bytes, int offset) {
        if (bytes.length - offset < 8) {
            throw new PickleException("too few bytes to convert to long");
        }
        long i = bytes[7 + offset] & 0xff;
        i <<= 8;
        i |= bytes[6 + offset] & 0xff;
        i <<= 8;
        i |= bytes[5 + offset] & 0xff;
        i <<= 8;
        i |= bytes[4 + offset] & 0xff;
        i <<= 8;
        i |= bytes[3 + offset] & 0xff;
        i <<= 8;
        i |= bytes[2 + offset] & 0xff;
        i <<= 8;
        i |= bytes[1 + offset] & 0xff;
        i <<= 8;
        i |= bytes[offset] & 0xff;
        return i;
    }


    public static long bytes2Uint(byte[] bytes, int offset) {
        if (bytes.length - offset < 4) {
            throw new PickleException("too few bytes to convert to long");
        }
        long i = bytes[3 + offset] & 0xff;
        i <<= 8;
        i |= bytes[2 + offset] & 0xff;
        i <<= 8;
        i |= bytes[1 + offset] & 0xff;
        i <<= 8;
        i |= bytes[offset] & 0xff;
        return i;
    }


    public static byte[] integer2Bytes(int i) {
        final byte[] b = new byte[4];
        b[0] = (byte) (i & 0xff);
        i >>= 8;
        b[1] = (byte) (i & 0xff);
        i >>= 8;
        b[2] = (byte) (i & 0xff);
        i >>= 8;
        b[3] = (byte) (i & 0xff);
        return b;
    }

    public static byte[] double2Bytes(double d) {
        long bits = Double.doubleToRawLongBits(d);
        final byte[] b = new byte[8];
        b[7] = (byte) (bits & 0xff);
        bits >>= 8;
        b[6] = (byte) (bits & 0xff);
        bits >>= 8;
        b[5] = (byte) (bits & 0xff);
        bits >>= 8;
        b[4] = (byte) (bits & 0xff);
        bits >>= 8;
        b[3] = (byte) (bits & 0xff);
        bits >>= 8;
        b[2] = (byte) (bits & 0xff);
        bits >>= 8;
        b[1] = (byte) (bits & 0xff);
        bits >>= 8;
        b[0] = (byte) (bits & 0xff);
        return b;
    }


    public static double bytes2Double(byte[] bytes, int offset) {
        try {
            long result = bytes[offset] & 0xff;
            result <<= 8;
            result |= bytes[1 + offset] & 0xff;
            result <<= 8;
            result |= bytes[2 + offset] & 0xff;
            result <<= 8;
            result |= bytes[3 + offset] & 0xff;
            result <<= 8;
            result |= bytes[4 + offset] & 0xff;
            result <<= 8;
            result |= bytes[5 + offset] & 0xff;
            result <<= 8;
            result |= bytes[6 + offset] & 0xff;
            result <<= 8;
            result |= bytes[7 + offset] & 0xff;
            return Double.longBitsToDouble(result);
        } catch (IndexOutOfBoundsException x) {
            throw new PickleException("decoding double: too few bytes");
        }
    }


    public static float bytes2Float(byte[] bytes, int offset) {
        try {
            int result = bytes[offset] & 0xff;
            result <<= 8;
            result |= bytes[1 + offset] & 0xff;
            result <<= 8;
            result |= bytes[2 + offset] & 0xff;
            result <<= 8;
            result |= bytes[3 + offset] & 0xff;
            return Float.intBitsToFloat(result);
        } catch (IndexOutOfBoundsException x) {
            throw new PickleException(String.format("decoding float: too few bytes, %s", x));
        }
    }


    public static Number decodeLong(byte[] data) {
        if (data.length == 0) {
            return 0L;
        }
        byte[] data2 = new byte[data.length];
        for (int i = 0; i < data.length; ++i) {
            data2[data.length - i - 1] = data[i];
        }
        BigInteger bigint = new BigInteger(data2);
        return optimizeBigint(bigint);
    }


    public static byte[] encodeLong(BigInteger big) {
        byte[] data = big.toByteArray();
        byte[] data2 = new byte[data.length];
        for (int i = 0; i < data.length; ++i) {
            data2[data.length - i - 1] = data[i];
        }
        return data2;
    }

    public static Number optimizeBigint(BigInteger bigint) {
        final BigInteger maxLong = BigInteger.valueOf(Long.MAX_VALUE);
        final BigInteger minLong = BigInteger.valueOf(Long.MIN_VALUE);
        switch (bigint.signum()) {
            case 0:
                return 0L;
            case 1:
                if (bigint.compareTo(maxLong) <= 0) {
                    return bigint.longValue();
                }
                break;
            case -1:
                if (bigint.compareTo(minLong) >= 0) {
                    return bigint.longValue();
                }
                break;
            default:
                break;
        }
        return bigint;
    }

    public static String rawStringFromBytes(byte[] data) {
        StringBuilder str = new StringBuilder(data.length);
        for (byte b : data) {
            str.append((char) (b & 0xff));
        }
        return str.toString();
    }

    public static byte[] str2bytes(String str) throws IOException {
        byte[] b = new byte[str.length()];
        for (int i = 0; i < str.length(); ++i) {
            char c = str.charAt(i);
            if (c > 255) {
                throw new UnsupportedEncodingException("string contained a char > 255,"
                    + " cannot convert to bytes");
            }
            b[i] = (byte) c;
        }
        return b;
    }


    public static String decodeEscaped(String str) {
        if (str.indexOf('\\') == -1) {
            return str;
        }
        StringBuilder sb = new StringBuilder(str.length());
        for (int i = 0; i < str.length(); ++i) {
            char c = str.charAt(i);
            if (c == '\\') {
                char c2 = str.charAt(++i);
                switch (c2) {
                    case '\\':
                        sb.append(c);
                        break;
                    case 'x':
                        char h1 = str.charAt(++i);
                        char h2 = str.charAt(++i);
                        c2 = (char) Integer.parseInt("" + h1 + h2, 16);
                        sb.append(c2);
                        break;
                    case 'n':
                        sb.append('\n');
                        break;
                    case 'r':
                        sb.append('\r');
                        break;
                    case 't':
                        sb.append('\t');
                        break;
                    case '\'':
                        sb.append('\'');
                        break;
                    default:
                        if (str.length() > 80) {
                            str = str.substring(0, 80);
                        }
                        throw new PickleException("invalid escape sequence char \'"
                            + (c2) + "\' in string \"" + str + " [...]\" (possibly truncated)");
                }
            } else {
                sb.append(str.charAt(i));
            }
        }
        return sb.toString();
    }


    public static String decodeUnicodeEscaped(String str) {
        if (str.indexOf('\\') == -1) {
            return str;
        }
        StringBuilder sb = new StringBuilder(str.length());
        for (int i = 0; i < str.length(); ++i) {
            char c = str.charAt(i);
            if (c == '\\') {
                char c2 = str.charAt(++i);
                switch (c2) {
                    case '\\':
                        sb.append(c);
                        break;
                    case 'u': {
                        char h1 = str.charAt(++i);
                        char h2 = str.charAt(++i);
                        char h3 = str.charAt(++i);
                        char h4 = str.charAt(++i);
                        c2 = (char) Integer.parseInt("" + h1 + h2 + h3 + h4, 16);
                        sb.append(c2);
                        break;
                    }
                    case 'U': {
                        char h1 = str.charAt(++i);
                        char h2 = str.charAt(++i);
                        char h3 = str.charAt(++i);
                        char h4 = str.charAt(++i);
                        char h5 = str.charAt(++i);
                        char h6 = str.charAt(++i);
                        char h7 = str.charAt(++i);
                        char h8 = str.charAt(++i);
                        String encoded = "" + h1 + h2 + h3 + h4 + h5 + h6 + h7 + h8;
                        String s = new String(Character.toChars(Integer.parseInt(encoded, 16)));
                        sb.append(s);
                        break;
                    }
                    case 'n':
                        sb.append('\n');
                        break;
                    case 'r':
                        sb.append('\r');
                        break;
                    case 't':
                        sb.append('\t');
                        break;
                    default:
                        if (str.length() > 80) {
                            str = str.substring(0, 80);
                        }
                        throw new PickleException("invalid escape sequence char "
                            + "\'" + (c2) + "\' in string \"" + str + " [...]\" (possibly truncated)");
                }
            } else {
                sb.append(str.charAt(i));
            }
        }
        return sb.toString();
    }
}
