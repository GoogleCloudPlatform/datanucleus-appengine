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
 * EasyMock matcher that verifies that the failoverMs property
 * is properly set on a {@link DatastorePb.Query}.
 *
 * @author Max Ross <max.ross@gmail.com>
 */
final class FailoverMsMatcher implements IArgumentMatcher {
  private final Long expectedFailoverMs;

  FailoverMsMatcher(Long expectedFailoverMs) {
    this.expectedFailoverMs = expectedFailoverMs;
  }

  public boolean matches(Object argument) {
    DatastorePb.Query query = new DatastorePb.Query();
    query.mergeFrom((byte[]) argument);
    if (expectedFailoverMs == null) {
      return !query.hasFailoverMs();
    }
    return expectedFailoverMs.equals(query.getFailoverMs());
  }

  public void appendTo(StringBuffer buffer) {
    buffer.append("FailoverMs Matcher: " + expectedFailoverMs);
  }

  public static byte[] eqFailoverMs(Long failoverMs) {
    EasyMock.reportMatcher(new FailoverMsMatcher(failoverMs));
    return null;
  }

}
