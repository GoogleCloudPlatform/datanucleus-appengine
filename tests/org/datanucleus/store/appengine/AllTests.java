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
package org.datanucleus.store.appengine;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.datanucleus.store.appengine.jdo.DatastoreJDOPersistenceManagerFactoryTest;
import org.datanucleus.store.appengine.jpa.DatastoreEntityManagerFactoryTest;
import org.datanucleus.store.appengine.query.JDOQLQueryTest;
import org.datanucleus.store.appengine.query.JPQLQueryTest;
import org.datanucleus.store.appengine.query.StreamingQueryResultTest;

/**
 * All tests for the app engine datanucleus plugin.
 * This will be difficult to keep in sync but we'll do our best.
 *
 * @author Max Ross <maxr@google.com>
 */
public class AllTests {
  public static Test suite() {
    TestSuite suite = new TestSuite();
    suite.addTestSuite(DatastoreFieldManagerTest.class);
    suite.addTestSuite(DatastoreJDOPersistenceManagerFactoryTest.class);
    suite.addTestSuite(DatastoreEntityManagerFactoryTest.class);
    suite.addTestSuite(JDOQLQueryTest.class);
    suite.addTestSuite(JPQLQueryTest.class);
    suite.addTestSuite(StreamingQueryResultTest.class);
    suite.addTestSuite(SerializationTest.class);
    suite.addTestSuite(SerializationManagerTest.class);
    suite.addTestSuite(JDOFetchTest.class);
    suite.addTestSuite(JDOInsertionTest.class);
    suite.addTestSuite(JDOUpdateTest.class);
    suite.addTestSuite(JDODeleteTest.class);
    suite.addTestSuite(JDOTransactionTest.class);
    suite.addTestSuite(JPAFetchTest.class);
    suite.addTestSuite(JPAInsertionTest.class);
    suite.addTestSuite(JPAUpdateTest.class);
    suite.addTestSuite(JPADeleteTest.class);
    suite.addTestSuite(JPATransactionTest.class);
    suite.addTestSuite(JDOAncestorTest.class);
    suite.addTestSuite(JPAAncestorTest.class);
    suite.addTestSuite(JDOTableAndColumnTest.class);
    suite.addTestSuite(JPAOneToOneTest.class);
    suite.addTestSuite(JDOOneToOneTest.class);
    suite.addTestSuite(JPAImplicitEntityGroupTest.class);
    suite.addTestSuite(JDOImplicitEntityGroupTest.class);
    suite.addTestSuite(JPAOneToManyListTest.class);
    suite.addTestSuite(JPAOneToManySetTest.class);
    suite.addTestSuite(JDOOneToManyListTest.class);
    suite.addTestSuite(JDOOneToManySetTest.class);
    // TODO(maxr) Reenable once we reenable support for arrays.
//    suite.addTestSuite(JDOOneToManyArrayTest.class);
    suite.addTestSuite(DatastoreQueryExpressionTest.class);
    suite.addTestSuite(JDODataSourceConfigTest.class);
    suite.addTestSuite(JPADataSourceConfigTest.class);
    suite.addTestSuite(JDOEnumTest.class);
    suite.addTestSuite(JPAEnumTest.class);
    suite.addTestSuite(JDONullValueTest.class);
    suite.addTestSuite(JPANullValueTest.class);
    suite.addTestSuite(TypeConversionUtilsTest.class);
    suite.addTestSuite(JDOMakeTransientTest.class);
    suite.addTestSuite(JDOPrimaryKeyTest.class);
    suite.addTestSuite(JDOMetaDataValidatorTest.class);
    suite.addTestSuite(JPAPrimaryKeyTest.class);
    suite.addTestSuite(JPAMetaDataValidatorTest.class);
    suite.addTestSuite(EntityUtilsTest.class);
    suite.addTestSuite(JDOBytesTest.class);
    suite.addTestSuite(JPABytesTest.class);
    suite.addTestSuite(JDOConcurrentModificationTest.class);
    suite.addTestSuite(JPAConcurrentModificationTest.class);
    suite.addTestSuite(JDOEmbeddedTest.class);
    suite.addTestSuite(JPAEmbeddedTest.class);
    suite.addTestSuite(JDOFetchGroupTest.class);
    suite.addTestSuite(JDOAttachDetachTest.class);
    suite.addTestSuite(JPAAttachDetachTest.class);
    suite.addTestSuite(JDOSubclassTest.class);
    suite.addTestSuite(JPASubclassTest.class);
    suite.addTestSuite(JPALobTest.class);
    return suite;
  }
}
