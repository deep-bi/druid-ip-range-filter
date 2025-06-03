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
package bi.deep.util;

import inet.ipaddr.IPAddressString;
import inet.ipaddr.format.IPAddressRange;

public class IPAddressTest {

    public static void main(String[] args) {
        String val = "192.168.0.0/24";

        IPAddressRange address = new IPAddressString(val).getAddress();

        System.out.println(address.toString());
        System.out.println(address.toSequentialRange());

        val = "192.168.0.0";
        address = new IPAddressString(val).getAddress();
        System.out.println(address.toString());

        val = "192.168.0.10";
        address = new IPAddressString(val).getAddress().spanWithRange(new IPAddressString(val).getAddress());

        System.out.println(address.toString());
    }
}
