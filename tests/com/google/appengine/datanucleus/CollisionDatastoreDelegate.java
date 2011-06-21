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

import java.util.ConcurrentModificationException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class CollisionDatastoreDelegate extends ExceptionThrowingDatastoreDelegate {

  public CollisionDatastoreDelegate(ApiProxy.Delegate inner, ExceptionPolicy policy) {
    super(inner, policy);
  }

  public CollisionDatastoreDelegate(ApiProxy.Delegate inner) {
    this(inner, ALWAYS_COLLIDE);
  }

  public static final ExceptionPolicy ALWAYS_COLLIDE = new BaseExceptionPolicy() {
    protected void doIntercept(String methodName) {
      throw new ConcurrentModificationException();
    }
  };
}