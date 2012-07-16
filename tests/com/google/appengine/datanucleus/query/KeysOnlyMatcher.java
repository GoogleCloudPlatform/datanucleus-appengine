/*
 * Copyright (C) 2010 Google Inc
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
package com.google.appengine.datanucleus.query;

import com.google.apphosting.api.DatastorePb;

import org.easymock.EasyMock;
import org.easymock.IArgumentMatcher;

/**
 * @author Max Ross <max.ross@gmail.com>
 */
public class KeysOnlyMatcher implements IArgumentMatcher {
  private final boolean expectedKeysOnly;

  public KeysOnlyMatcher(boolean expectedKeysOnly) {
    this.expectedKeysOnly = expectedKeysOnly;
  }

  public boolean matches(Object argument) {
    DatastorePb.Query query = new DatastorePb.Query();
    query.mergeFrom((byte[]) argument);
    return expectedKeysOnly == query.isKeysOnly();
  }

  public void appendTo(StringBuffer buffer) {
    buffer.append("Keys Only: " + expectedKeysOnly);
  }

  public static byte[] eqKeysOnly(boolean expectedKeysOnly) {
    EasyMock.reportMatcher(new KeysOnlyMatcher(expectedKeysOnly));
    return null;
  }
}
