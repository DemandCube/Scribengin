/*
 * Copyright 2014 Higher Frequency Trading
 * <p/>
 * http://www.higherfrequencytrading.com
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.neverwinterdp.buffer.chronicle;

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

import net.openhft.lang.io.IOTools;

import org.junit.Rule;
import org.junit.rules.TestName;

public class VanillaChronicleTestBase {
    protected static final String TMP_DIR = System.getProperty("java.io.tmpdir");

    @Rule
    public final TestName testName = new TestName();

    protected synchronized String getTestPath() {
        final String path = TMP_DIR + "/vc-" + testName.getMethodName();
        IOTools.deleteDir(path);

        return path;
    }

    protected synchronized String getTestPath(String suffix) {
        final String path = TMP_DIR + "/vc-" + testName.getMethodName() + suffix;
        IOTools.deleteDir(path);

        return path;
    }

    protected int getPID() {
        return Integer.parseInt(getPIDAsString());
    }

    protected String getPIDAsString() {
        final String name = ManagementFactory.getRuntimeMXBean().getName();
        return name.split("@")[0];
    }

    protected void sleep(long timeout, TimeUnit unit) {
        sleep(TimeUnit.MILLISECONDS.convert(timeout,unit));
    }

    protected void sleep(long timeout) {
        try {
            Thread.sleep(timeout);
        } catch (InterruptedException e) {
        }
    }
}
