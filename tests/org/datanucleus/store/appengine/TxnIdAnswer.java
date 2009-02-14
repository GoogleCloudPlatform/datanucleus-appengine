// Copyright 2009 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

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
