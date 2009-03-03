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

package org.datanucleus.store.appengine;

import com.google.apphosting.api.ApiProxy;

import java.util.ConcurrentModificationException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class CollidingUpdateDatastoreDelegate implements ApiProxy.Delegate {
  private final ApiProxy.Delegate inner;
  private final CollisionPolicy policy;

  public CollidingUpdateDatastoreDelegate(ApiProxy.Delegate inner, CollisionPolicy policy) {
    this.inner = inner;
    this.policy = policy;
  }

  public CollidingUpdateDatastoreDelegate(ApiProxy.Delegate inner) {
    this(inner, ALWAYS_COLLIDE);
  }

  public byte[] makeSyncCall(ApiProxy.Environment environment, String packageName,
      String methodName, byte[] request) throws ApiProxy.ApiProxyException {
    policy.collide(methodName);
    return inner.makeSyncCall(environment, packageName, methodName, request);
  }

  public void log(ApiProxy.Environment environment, ApiProxy.LogRecord logRecord) {
    inner.log(environment, logRecord);
  }

  public interface CollisionPolicy {
    void collide(String methodName);
  }

  public static abstract class BaseCollisionPolicy implements CollisionPolicy {

    public final void collide(String methodName) {
      if (methodName.equals("Put") || methodName.equals("Commit") || methodName.equals("Delete")) {
        doCollide(methodName);
      }
    }

    protected abstract void doCollide(String methodName);
  }

  public static final CollisionPolicy ALWAYS_COLLIDE = new BaseCollisionPolicy() {
    protected void doCollide(String methodName) {
      throw new ConcurrentModificationException();
    }
  };
}
