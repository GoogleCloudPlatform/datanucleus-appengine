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
package org.datanucleus.test;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class HasLob {

  @Id
  @GeneratedValue(strategy= GenerationType.IDENTITY)
  private Long id;

  @Lob
  private String bigString;

  @Lob
  private byte[] bigByteArray;

  public Long getId() {
    return id;
  }

  public String getBigString() {
    return bigString;
  }

  public void setBigString(String bigString) {
    this.bigString = bigString;
  }

  public byte[] getBigByteArray() {
    return bigByteArray;
  }

  public void setBigByteArray(byte[] bigByteArray) {
    this.bigByteArray = bigByteArray;
  }
}
