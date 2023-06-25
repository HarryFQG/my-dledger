/*
 * Copyright 2017-2022 The DLedger Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.example.register.protocol;

import io.openmessaging.storage.dledger.ReadMode;
import io.openmessaging.storage.dledger.protocol.userdefine.UserDefineRequest;

public class RegisterReadRequest extends UserDefineRequest {

    private final Integer key;

    private ReadMode readMode = ReadMode.RAFT_LOG_READ;

    public RegisterReadRequest(int key) {
        this.key = key;
    }

    public RegisterReadRequest(int key, ReadMode readMode) {
        this.key = key;
        this.readMode = readMode;
    }

    public void setReadMode(ReadMode readMode) {
        this.readMode = readMode;
    }

    public ReadMode getReadMode() {
        return readMode;
    }

    public Integer getKey() {
        return key;
    }

    @Override
    public int getRequestTypeCode() {
        return RegisterRequestTypeCode.READ.ordinal();
    }
}
