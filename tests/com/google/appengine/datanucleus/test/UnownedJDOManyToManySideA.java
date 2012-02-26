/**********************************************************************
Copyright (c) 2012 Google Inc.

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
package com.google.appengine.datanucleus.test;

import java.util.HashSet;
import java.util.Set;

import javax.jdo.annotations.Element;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.datanucleus.annotations.Unowned;

@PersistenceCapable(detachable="true")
public class UnownedJDOManyToManySideA {
  @Persistent(valueStrategy=IdGeneratorStrategy.IDENTITY, primaryKey="true")
  private Key key;

  @Unowned
  @Element(mappedBy="as")
  private Set<UnownedJDOManyToManySideB> bs = new HashSet<UnownedJDOManyToManySideB>();

  public Key getKey() {
    return key;
  }

  public Set<UnownedJDOManyToManySideB> getBs() {
    return bs;
  }
}
