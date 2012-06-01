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
package com.google.appengine.datanucleus.test.jpa;

import javax.persistence.Embeddable;

/**
 * The runtime enhancer has problems with static inner embedded classes that
 * have embeddable super classes, so this class gets its own file.
 *
 * @author Max Ross <max.ross@gmail.com>
*/
@Embeddable
public class IsEmbeddedWithEmbeddedSuperclass2 extends SubclassesJPA.IsEmbeddedBase2 {

  private String val3;

  public String getVal3() {
    return val3;
  }

  public void setVal3(String val3) {
    this.val3 = val3;
  }
}
