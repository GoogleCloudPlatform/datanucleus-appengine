/*
 * /**********************************************************************
 * Copyright (c) 2009 Google Inc.
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
 * **********************************************************************/

package com.google.appengine.datanucleus;

import com.google.apphosting.api.ApiProxy;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * @author Max Ross <maxr@google.com>
 */
public class ExceptionThrowingDatastoreDelegate implements ApiProxy.Delegate {
  private final ApiProxy.Delegate inner;
  private final ExceptionPolicy policy;

  public ExceptionThrowingDatastoreDelegate(ApiProxy.Delegate inner, ExceptionPolicy policy) {
    this.inner = inner;
    this.policy = policy;
  }

  public byte[] makeSyncCall(ApiProxy.Environment environment, String packageName,
      String methodName, byte[] request) throws ApiProxy.ApiProxyException {
    policy.intercept(methodName);
    return inner.makeSyncCall(environment, packageName, methodName, request);
  }

  public Future<byte[]> makeAsyncCall(ApiProxy.Environment environment, String packageName, String methodName,
                              byte[] request, ApiProxy.ApiConfig apiConfig) {
    policy.intercept(methodName);
    return inner.makeAsyncCall(environment, packageName, methodName, request, apiConfig);
  }

  public void log(ApiProxy.Environment environment, ApiProxy.LogRecord logRecord) {
    inner.log(environment, logRecord);
  }

  public void flushLogs(ApiProxy.Environment environment) {
    inner.flushLogs(environment);
  }

  public List<Thread> getRequestThreads(ApiProxy.Environment environment) {
    return inner.getRequestThreads(environment);
  }

  public interface ExceptionPolicy {
    void intercept(String methodName);
  }

  public static abstract class BaseExceptionPolicy implements ExceptionPolicy {

    private static final Set<String> RPCS_TO_INTERCEPT =
        Utils.newHashSet("Put", "Delete", "Commit", "RunQuery", "Next");

    public final void intercept(String methodName) {
      if (RPCS_TO_INTERCEPT.contains(methodName)) {
        doIntercept(methodName);
      }
    }

    protected abstract void doIntercept(String methodName);
  }

  public ApiProxy.Delegate getInner() {
    return inner;
  }
}
