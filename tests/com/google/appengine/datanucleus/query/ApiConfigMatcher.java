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

import com.google.apphosting.api.ApiProxy;

import org.easymock.EasyMock;
import org.easymock.IArgumentMatcher;

/**
 * EasyMock matcher for ApiConfig.  Delete this once ApiConfig implements
 * equals().
 *
 * @author Max Ross <max.ross@gmail.com>
 */
final class ApiConfigMatcher implements IArgumentMatcher {
  private final ApiProxy.ApiConfig expectedApiConfig;

  ApiConfigMatcher(ApiProxy.ApiConfig expectedApiConfig) {
    this.expectedApiConfig = expectedApiConfig;
  }

  public boolean matches(Object argument) {
    ApiProxy.ApiConfig actualApiConfig = (ApiProxy.ApiConfig) argument;
    if (expectedApiConfig.getDeadlineInSeconds() == null) {
      return actualApiConfig.getDeadlineInSeconds() == null;
    }
    return expectedApiConfig.getDeadlineInSeconds().equals(actualApiConfig.getDeadlineInSeconds());
  }

  public void appendTo(StringBuffer buffer) {
    buffer.append("ApiConfig Matcher: " + expectedApiConfig.getDeadlineInSeconds());
  }

  public static ApiProxy.ApiConfig eqApiConfig(ApiProxy.ApiConfig apiConfig) {
    EasyMock.reportMatcher(new ApiConfigMatcher(apiConfig));
    return null;
  }

}
