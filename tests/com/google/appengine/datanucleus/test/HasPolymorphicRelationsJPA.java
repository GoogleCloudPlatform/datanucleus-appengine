/**********************************************************************
Copyright (c) 2011 Google Inc.

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

import com.google.appengine.api.datastore.Key;
import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildJPA.BidirTop;
import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildJPA.BidirTopLongPk;
import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildJPA.BidirTopUnencodedStringPk;
import com.google.appengine.datanucleus.test.UnidirectionalSingeTableChildJPA.UnidirTop;


import java.util.Collection;


public interface HasPolymorphicRelationsJPA {

  public interface HasOneToManyJPA {
    String getId();
    Collection<BidirTop> getBidirChildren();
    Collection<UnidirTop> getUnidirChildren();
    Collection<HasKeyPkJPA> getHasKeyPks();
    void setVal(String s);
    void nullUnidirChildren();
    void nullHasKeyPks();
    void nullBidirChildren();
  }
  
  public interface HasOneToManyKeyPkJPA {
    Key getId();
    Collection<UnidirTop> getUnidirChildren();
  }
  
  public interface HasOneToManyLongPkJPA {
    Long getId();
    Collection<UnidirTop> getUnidirChildren();
    Collection<BidirTopLongPk> getBidirChildren();
  }
  
  public interface HasOneToManyUnencodedStringPkJPA {
    String getId();
    void setId(String id);
    Collection<UnidirTop> getUnidirChildren();
    Collection<BidirTopUnencodedStringPk> getBidirChildren();
  }
}
