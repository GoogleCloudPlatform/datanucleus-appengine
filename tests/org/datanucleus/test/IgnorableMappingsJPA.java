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

import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;

/**
 * @author Max Ross <maxr@google.com>
 */
public class IgnorableMappingsJPA {

  @Entity
  public static class OneToManyParentWithEagerlyFetchedChildList {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    public Long id;

    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
    private List<HasKeyPkJPA> children;
  }

  @Entity
  public static class OneToManyParentWithEagerlyFetchedChild {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    public Long id;

    @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
    private HasKeyPkJPA child;
  }
}