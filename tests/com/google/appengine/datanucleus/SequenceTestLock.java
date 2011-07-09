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
package com.google.appengine.datanucleus;

import java.util.concurrent.Semaphore;

/**
 * A lock that we use to prevent sequence tests from running concurrently.
 *
 * @author Max Ross <max.ross@gmail.com>
 */
public final class SequenceTestLock {
  private SequenceTestLock() {}
  
  public static final Semaphore LOCK = new Semaphore(1);
}
