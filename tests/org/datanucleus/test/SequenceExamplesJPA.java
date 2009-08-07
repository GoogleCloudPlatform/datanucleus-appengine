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
import javax.persistence.SequenceGenerator;

/**
 * @author Max Ross <maxr@google.com>
 */
public final class SequenceExamplesJPA {

  private SequenceExamplesJPA() {}
  
  @Entity
  public static class HasSequence {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE)
    private Long id;

    private String val;

    public Long getId() {
      return id;
    }

    public void setId(Long id) {
      this.id = id;
    }

    public String getVal() {
      return val;
    }

    public void setVal(String val) {
      this.val = val;
    }
  }

  @Entity
  public static class HasSequenceWithSequenceGenerator {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "yar")
    @SequenceGenerator(name = "yar", sequenceName = "that", allocationSize = 12)
    private Long id;

    private String val;

    public Long getId() {
      return id;
    }

    public void setId(Long id) {
      this.id = id;
    }

    public String getVal() {
      return val;
    }

    public void setVal(String val) {
      this.val = val;
    }
  }

  @Entity
  public static class HasSequenceWithNoSequenceName {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "yar")
    @SequenceGenerator(name = "yar", allocationSize = 12)
    private Long id;

    public Long getId() {
      return id;
    }

    public void setId(Long id) {
      this.id = id;
    }
  }
}
