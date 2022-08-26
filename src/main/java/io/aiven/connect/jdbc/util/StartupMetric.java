/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.connect.jdbc.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et
 * dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum.
 */
public class StartupMetric implements StartupMetricMBean {

    private static final Logger log = LoggerFactory.getLogger(StartupMetric.class);

    private long counter = -1;

    public StartupMetric() {
        super();
    }

    @Override
    public Long getCounter() {
        log.debug("getCounter: " + counter);
        return counter;
    }

    public void updateCounter(final Long counter) {
        log.info("setCounter: " + counter);
        if (counter != null) {
            this.counter = counter.intValue();
        }
    }
}
