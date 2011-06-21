/**********************************************************************
Copyright (c) 2009 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**********************************************************************/
package com.google.appengine.datanucleus;

import org.easymock.IAnswer;

/**
 * Useful for tests that need to configure mock transactions.
 *
 * @see {@link JDOTransactionTest}
 * @see {@link JPATransactionTest}
 *
 * @author Max Ross <maxr@google.com>
 */
class TxnIdAnswer implements IAnswer<String> {
  private String expectedTxnId;
  public String answer() {
    return expectedTxnId;
  }

  public void setExpectedTxnId(String expectedTxnId) {
    this.expectedTxnId = expectedTxnId;
  }
}
