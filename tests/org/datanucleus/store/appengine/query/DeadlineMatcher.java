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
package org.datanucleus.store.appengine.query;

import com.google.apphosting.api.ApiProxy;

import org.easymock.EasyMock;
import org.easymock.IArgumentMatcher;

/**
 * EasyMock matcher that verifies that the deadline of the current environment
 * matches the deadline of the provided environment.
 *
 * @author Max Ross <max.ross@gmail.com>
*/
final class DeadlineMatcher implements IArgumentMatcher {
  private final Double expectedDeadline;

  DeadlineMatcher(Double expectedDeadline) {
    this.expectedDeadline = expectedDeadline;
  }

  public boolean matches(Object argument) {
    ApiProxy.Environment env = (ApiProxy.Environment) argument;
    Double deadline =
        (Double) env.getAttributes().get(ApiProxy.class.getName() + ".api_deadline_key");
    if (deadline == null) {
      return expectedDeadline == null;
    }
    return deadline.equals(expectedDeadline);
  }

  public void appendTo(StringBuffer buffer) {
    buffer.append("Env Matcher");
  }

  public static ApiProxy.Environment eqDeadline(Double expectedDeadline) {
    EasyMock.reportMatcher(new DeadlineMatcher(expectedDeadline));
    return null;
  }
}
