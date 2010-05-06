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

import javax.jdo.annotations.Extension;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;
import javax.jdo.annotations.Sequence;
import javax.jdo.annotations.SequenceStrategy;

/**
 * @author Max Ross <maxr@google.com>
 */
public final class SequenceExamplesJDO {

  private SequenceExamplesJDO() {
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class HasSequence {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.SEQUENCE)
    private Long id;

    private String val;

    public Long getId() {
      return id;
    }

    public String getVal() {
      return val;
    }

    public void setVal(String val) {
      this.val = val;
    }
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  @Sequence(name = "jdo1", datastoreSequence = "jdothat", strategy = SequenceStrategy.NONTRANSACTIONAL,
            extensions = @Extension(vendorName = "datanucleus", key="key-cache-size", value="12"))
  public static class HasSequenceWithSequenceGenerator {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.SEQUENCE, sequence = "jdo1")
    private Long id;

    private String val;

    public Long getId() {
      return id;
    }

    public String getVal() {
      return val;
    }

    public void setVal(String val) {
      this.val = val;
    }
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  @Sequence(name = "jdo2", strategy = SequenceStrategy.NONTRANSACTIONAL,
            extensions = @Extension(vendorName = "datanucleus", key="key-cache-size", value="12"))
  public static class HasSequenceWithNoSequenceName {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.SEQUENCE, sequence = "jdo2")
    private Long id;

    public Long getId() {
      return id;
    }

    public void setId(Long id) {
      this.id = id;
    }
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class HasSequenceWithUnencodedStringPk {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.SEQUENCE)
    private String id;

    public String getId() {
      return id;
    }
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class HasSequenceOnNonPkFields {

    @PrimaryKey
    private String id;

    @Persistent(valueStrategy = IdGeneratorStrategy.SEQUENCE)
    private long val1;

    @Persistent(valueStrategy = IdGeneratorStrategy.SEQUENCE)
    private long val2;

    public void setId(String id) {
      this.id = id;
    }

    public String getId() {
      return id;
    }

    public long getVal1() {
      return val1;
    }

    public long getVal2() {
      return val2;
    }
  }
}