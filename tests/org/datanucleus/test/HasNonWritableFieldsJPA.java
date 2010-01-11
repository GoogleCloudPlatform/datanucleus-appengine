/*
 * Copyright (C) 2009 Max Ross.
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
package org.datanucleus.test;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

/**
 * @author Max Ross <max.ross@gmail.com>
 */
@Entity
public class HasNonWritableFieldsJPA {
  @Id
  @GeneratedValue(strategy= GenerationType.IDENTITY)
  private Long id;

  @Column(insertable = false)
  private String notInsertable;

  @Column(updatable = false)
  private String notUpdatable;

  @Column(insertable = false, updatable = false)
  private String notWritable;

  public Long getId() {
    return id;
  }

  public String getNotInsertable() {
    return notInsertable;
  }

  public void setNotInsertable(String notInsertable) {
    this.notInsertable = notInsertable;
  }

  public String getNotUpdatable() {
    return notUpdatable;
  }

  public void setNotUpdatable(String notUpdatable) {
    this.notUpdatable = notUpdatable;
  }

  public String getNotWritable() {
    return notWritable;
  }

  public void setNotWritable(String notWritable) {
    this.notWritable = notWritable;
  }
}
