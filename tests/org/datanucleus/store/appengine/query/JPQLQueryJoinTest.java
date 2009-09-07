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
package org.datanucleus.store.appengine.query;

import com.google.appengine.api.datastore.Entity;

import org.datanucleus.store.appengine.JPATestCase;
import org.datanucleus.test.JoinsJPA.Student;
import static org.datanucleus.test.JoinsJPA.newCourseEntity;
import static org.datanucleus.test.JoinsJPA.newMajorEntity;
import static org.datanucleus.test.JoinsJPA.newStudentEntity;

import java.util.Collections;

import javax.persistence.Query;

/**
 * The JPQL query compiler isn't as lenient as the JDOQL query compiler, and
 * as a result we can't trick JPA into supporting joins on Key and List<Key>
 * members.  That's fair, since it's non-standard, but it means we won't be
 * able to support joins in JPQL until we the next storage version, where
 * we store child keys on the parent entity.  The tests in this class set up
 * test data via the low-level api as if we already support storing child keys
 * on the parent entity.
 *
 * @author Max Ross <maxr@google.com>
 */
public class JPQLQueryJoinTest extends JPATestCase {

  public void testJoinOnOneToMany_Simple() {
    Entity course1 = newCourseEntity("Biology");
    ldth.ds.put(course1);
    Entity course2 = newCourseEntity("Not Biology");
    ldth.ds.put(course2);
    Entity student = newStudentEntity(10, course1, course2);
    ldth.ds.put(student);
    beginTxn();
    Query q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.courses c where "
        + "c.department = 'Biology' and "
        + "s.grade = 10");
    
    assertEquals(student.getKey().getId(), ((Student) q.getSingleResult()).getId().longValue());
    commitTxn();
  }

  public void testJoinOnOneToMany_LegalOrderBy() {
    Entity course1 = newCourseEntity("Biology");
    ldth.ds.put(course1);
    Entity course2 = newCourseEntity("Not Biology");
    ldth.ds.put(course2);
    Entity student = newStudentEntity(10, course1, course2);
    ldth.ds.put(student);
    beginTxn();
    Query q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.courses c where "
        + "c.department = 'Biology' and "
        + "s.grade = 10 order by s.courses asc");
    assertEquals(student.getKey().getId(), ((Student) q.getSingleResult()).getId().longValue());
    commitTxn();
  }

  public void testJoinOnOneToMany_Offset() {
    Entity course1 = newCourseEntity("Biology");
    ldth.ds.put(course1);
    Entity course2 = newCourseEntity("Not Biology");
    ldth.ds.put(course2);
    Entity student = newStudentEntity(10, course1, course2);
    ldth.ds.put(student);
    Entity student2 = newStudentEntity(11, course1, course2);
    ldth.ds.put(student2);
    Entity student3 = newStudentEntity(10, course1, course2);
    ldth.ds.put(student3);
    beginTxn();
    Query q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.courses c where "
        + "c.department = 'Biology' and "
        + "s.grade = 10");
    q.setFirstResult(1);
    assertEquals(student3.getKey().getId(), ((Student) q.getSingleResult()).getId().longValue());
    q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.courses c where "
        + "c.department = 'Biology' and "
        + "s.grade = 10");
    q.setFirstResult(2);
    assertEquals(Collections.emptyList(), q.getResultList());
    commitTxn();
  }

  public void testJoinOnOneToMany_Limit() {
    Entity course1 = newCourseEntity("Biology");
    ldth.ds.put(course1);
    Entity course2 = newCourseEntity("Not Biology");
    ldth.ds.put(course2);
    Entity student = newStudentEntity(10, course1, course2);
    ldth.ds.put(student);
    Entity student2 = newStudentEntity(11, course1, course2);
    ldth.ds.put(student2);
    Entity student3 = newStudentEntity(10, course1, course2);
    ldth.ds.put(student3);
    beginTxn();
    Query q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.courses c where "
        + "c.department = 'Biology' and "
        + "s.grade = 10");
    q.setMaxResults(1);
    assertEquals(student.getKey().getId(), ((Student) q.getSingleResult()).getId().longValue());
    q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.courses c where "
        + "c.department = 'Biology' and "
        + "s.grade = 10");
    q.setMaxResults(0);
    assertEquals(Collections.emptyList(), q.getResultList());
    commitTxn();
  }

  public void testJoinOnOneToOne_Simple() {
    Entity major1 = newMajorEntity("Liberal Arts");
    ldth.ds.put(major1);
    Entity major2 = newMajorEntity("Engineering");
    ldth.ds.put(major2);
    Entity student1 = newStudentEntity(major1, 10);
    Entity student2 = newStudentEntity(major2, 10);
    ldth.ds.put(student1);
    ldth.ds.put(student2);
    beginTxn();
    Query q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.major m where "
        + "m.school = 'Liberal Arts' and "
        + "s.grade = 10");
    assertEquals(student1.getKey().getId(), ((Student) q.getSingleResult()).getId().longValue());
    commitTxn();
  }

  public void testJoinOnOneToMany_Illegal() {
    beginTxn();
    // all filters on parent must be equality filters
    Query q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.courses c where "
        + "c.department = 'Biology' and "
        + "s.grade > 10");
    try {
      q.getResultList();
      fail("expected exception");
    } catch (DatastoreQuery.UnsupportedDatastoreFeatureException udfe) {
      // good
    }

    // all filters on child must be equality filters
    q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.courses c where "
        + "c.department > 'Biology' and "
        + "s.grade = 10");
    try {
      q.getResultList();
      fail("expected exception");
    } catch (DatastoreQuery.UnsupportedDatastoreFeatureException udfe) {
      // good
    }

    // sort on parent can only be by join column in asc order
    q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.courses c where "
        + "c.department = 'Biology' and "
        + "s.grade = 10 order by s.grade");
    try {
      q.getResultList();
      fail("expected exception");
    } catch (DatastoreQuery.UnsupportedDatastoreFeatureException udfe) {
      // good
    }

    // sort is by the join column but in the wrong order
    q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.courses c where "
        + "c.department = 'Biology' and "
        + "s.grade = 10 order by s.courses desc");
    try {
      q.getResultList();
      fail("expected exception");
    } catch (DatastoreQuery.UnsupportedDatastoreFeatureException udfe) {
      // good
    }

    // can't sort by child property
    q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.courses c where "
        + "c.department = 'Biology' and "
        + "s.grade = 10 order by c.department");
    try {
      q.getResultList();
      fail("expected exception");
    } catch (DatastoreQuery.UnsupportedDatastoreFeatureException udfe) {
      // good
    }
    commitTxn();
  }

}