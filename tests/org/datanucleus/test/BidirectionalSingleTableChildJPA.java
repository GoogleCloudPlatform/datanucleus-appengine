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
package org.datanucleus.test;

import org.datanucleus.test.HasPolymorphicRelationsJPA.HasOneToManyJPA;
import org.datanucleus.test.HasPolymorphicRelationsJPA.HasOneToManyLongPkJPA;
import org.datanucleus.test.HasPolymorphicRelationsJPA.HasOneToManyUnencodedStringPkJPA;

public interface BidirectionalSingleTableChildJPA {
  
  public interface BidirTop {
    HasOneToManyJPA getParent();
    void setParent(HasOneToManyJPA parent);
    String getId();
    String getChildVal();
    void setChildVal(String childVal);
    int getPropertyCount();
  }
  
  public interface BidirMiddle extends BidirTop {
    void setMiddleChildVal(Long middleChildVal);
    Long getMiddleChildVal();
  }
  
  public interface BidirBottom extends BidirMiddle {
    void setBottomChildVal(Double bottomChildVal);
    Double getBottomChildVal();
  }

  public interface BidirTopLongPk {
    HasOneToManyLongPkJPA getParent();
    String getId();
    void setChildVal(String childVal);
    void setParent(HasOneToManyLongPkJPA parent);
    String getChildVal();
  }
  public interface BidirMiddleLongPk extends BidirTopLongPk {
  }
  public interface BidirBottomLongPk extends BidirMiddleLongPk {
  }
  
  public interface BidirTopUnencodedStringPk {
    HasOneToManyUnencodedStringPkJPA getParent();
    String getId();
    void setChildVal(String childVal);
    void setParent(HasOneToManyUnencodedStringPkJPA parent);
    String getChildVal();
    int getPropertyCount();
  }
  
  public interface BidirMiddleUnencodedStringPk extends BidirTopUnencodedStringPk {
    void setMiddleChildVal(String middleChildVal);
    String getMiddleChildVal();    
  }
  
  public interface BidirBottomUnencodedStringPk extends BidirMiddleUnencodedStringPk {
    void setBottomChildVal(String bottomChildVal);
    String getBottomChildVal();    
  }

}
