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

import junit.framework.TestCase;

import com.google.appengine.datanucleus.test.HasEnumJDO;

import java.util.Arrays;

/**
 * @author Max Ross <maxr@google.com>
 */
public class TypeConversionUtilsTest extends TestCase {
  public void testNullToArray() {
    TypeConversionUtils tcu = new TypeConversionUtils();
    @SuppressWarnings("unused")
    String[] stringArray =
        (String[]) tcu.convertDatastoreListToPojoArray(null, String.class);

    @SuppressWarnings("unused")
    HasEnumJDO.MyEnum[] enumArray = (HasEnumJDO.MyEnum[])
        tcu.convertDatastoreListToPojoArray(null, HasEnumJDO.MyEnum.class);

    @SuppressWarnings("unused")
    int[] intArray =
        (int[]) tcu.convertDatastoreListToPojoArray(null, Integer.TYPE);

  }

  public void testAsListBehavior() {
    Byte[] bArray = {0, 5, 8};
    assertEquals(3, Arrays.asList(bArray).size());
  }
}
