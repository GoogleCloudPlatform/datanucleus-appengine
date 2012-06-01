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
package com.google.appengine.datanucleus.query;

import static com.google.appengine.datanucleus.test.jpa.OwnedJoinsJPA.Course;
import static com.google.appengine.datanucleus.test.jpa.OwnedJoinsJPA.Major;
import static com.google.appengine.datanucleus.test.jpa.OwnedJoinsJPA.newCourse;
import static com.google.appengine.datanucleus.test.jpa.OwnedJoinsJPA.newMajor;
import static com.google.appengine.datanucleus.test.jpa.OwnedJoinsJPA.newStudent;

import com.google.appengine.datanucleus.jpa.JPATestCase;
import com.google.appengine.datanucleus.test.jpa.OwnedJoinsJPA.Student;

import java.util.Collections;

import javax.persistence.PersistenceException;
import javax.persistence.Query;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPQLQueryOwnedJoinTest extends JPATestCase {

  public void testJoinOnOneToMany_Simple() {
    Course course1 = newCourse("Biology");
    Course course2 = newCourse("Not Biology");
    Student student = newStudent(10, course1, course2);
    beginTxn();
    em.persist(student);
    commitTxn();
    beginTxn();
    Query q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.courses c where "
        + "c.department = 'Biology' and "
        + "s.grade = 10");
    
    assertEquals(student.getId(), ((Student) q.getSingleResult()).getId());
    commitTxn();
  }

  public void testJoinOnOneToMany_LegalOrderBy() {
    Course course1 = newCourse("Biology");
    Course course2 = newCourse("Not Biology");
    Student student = newStudent(10, course1, course2);
    beginTxn();
    em.persist(student);
    commitTxn();
    beginTxn();
    Query q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.courses c where "
        + "c.department = 'Biology' and "
        + "s.grade = 10 order by s.courses asc");
    assertEquals(student.getId(), ((Student) q.getSingleResult()).getId());
    commitTxn();
  }

  public void testJoinOnOneToMany_Offset() {
    Course course1 = newCourse("Biology");
    Course course2 = newCourse("Not Biology");
    Course course3 = newCourse("Biology");
    Course course4 = newCourse("Not Biology");
    Course course5 = newCourse("Biology");
    Course course6 = newCourse("Not Biology");
    Student student = newStudent(10, course1, course2);
    Student student2 = newStudent(11, course3, course4);
    Student student3 = newStudent(10, course5, course6);
    beginTxn();
    em.persist(student);
    commitTxn();
    beginTxn();
    em.persist(student2);
    commitTxn();
    beginTxn();
    em.persist(student3);
    commitTxn();
    beginTxn();
    Query q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.courses c where "
        + "c.department = 'Biology' and "
        + "s.grade = 10");
    q.setFirstResult(1);
    assertEquals(student3.getId(), ((Student) q.getSingleResult()).getId());
    q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.courses c where "
        + "c.department = 'Biology' and "
        + "s.grade = 10");
    q.setFirstResult(2);
    assertEquals(Collections.emptyList(), q.getResultList());
    commitTxn();
  }

  public void testJoinOnOneToMany_Limit() {
    Course course1 = newCourse("Biology");
    Course course2 = newCourse("Not Biology");
    Course course3 = newCourse("Biology");
    Course course4 = newCourse("Not Biology");
    Course course5 = newCourse("Biology");
    Course course6 = newCourse("Not Biology");
    Student student = newStudent(10, course1, course2);
    Student student2 = newStudent(11, course3, course4);
    Student student3 = newStudent(10, course5, course6);
    beginTxn();
    em.persist(student);
    commitTxn();
    beginTxn();
    em.persist(student2);
    commitTxn();
    beginTxn();
    em.persist(student3);
    commitTxn();
    beginTxn();
    Query q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.courses c where "
        + "c.department = 'Biology' and "
        + "s.grade = 10");
    q.setMaxResults(1);
    assertEquals(student.getId(), ((Student) q.getSingleResult()).getId());
    q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.courses c where "
        + "c.department = 'Biology' and "
        + "s.grade = 10");
    q.setMaxResults(0);
    assertEquals(Collections.emptyList(), q.getResultList());
    commitTxn();
  }

  public void testJoinOnOneToOne_Simple() {
    Major major1 = newMajor("Liberal Arts");
    Major major2 = newMajor("Engineering");
    Student student1 = newStudent(10, major1);
    Student student2 = newStudent(10, major2);
    beginTxn();
    em.persist(student1);
    commitTxn();
    beginTxn();
    em.persist(student2);
    commitTxn();
    beginTxn();
    Query q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.major m where "
        + "m.school = 'Liberal Arts' and "
        + "s.grade = 10");
    assertEquals(student1.getId(), ((Student) q.getSingleResult()).getId());
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
    } catch (PersistenceException pe) {
        if (pe.getCause() instanceof DatastoreQuery.UnsupportedDatastoreFeatureException) {
          // good
        }
        else {
          throw pe;
        }
    }

    // all filters on child must be equality filters
    q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.courses c where "
        + "c.department > 'Biology' and "
        + "s.grade = 10");
    try {
      q.getResultList();
      fail("expected exception");
    } catch (PersistenceException pe) {
        if (pe.getCause() instanceof DatastoreQuery.UnsupportedDatastoreFeatureException) {
          // good
        }
        else {
          throw pe;
        }
    }

    // sort on parent can only be by join column in asc order
    q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.courses c where "
        + "c.department = 'Biology' and "
        + "s.grade = 10 order by s.grade");
    try {
      q.getResultList();
      fail("expected exception");
    } catch (PersistenceException pe) {
        if (pe.getCause() instanceof DatastoreQuery.UnsupportedDatastoreFeatureException) {
          // good
        }
        else {
          throw pe;
        }
    }

    // sort is by the join column but in the wrong order
    q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.courses c where "
        + "c.department = 'Biology' and "
        + "s.grade = 10 order by s.courses desc");
    try {
      q.getResultList();
      fail("expected exception");
    } catch (PersistenceException pe) {
        if (pe.getCause() instanceof DatastoreQuery.UnsupportedDatastoreFeatureException) {
          // good
        }
        else {
          throw pe;
        }
    }

    // can't sort by child property
    q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.courses c where "
        + "c.department = 'Biology' and "
        + "s.grade = 10 order by c.department");
    try {
      q.getResultList();
      fail("expected exception");
    } catch (PersistenceException pe) {
        if (pe.getCause() instanceof DatastoreQuery.UnsupportedDatastoreFeatureException) {
          // good
        }
        else {
          throw pe;
        }
    }
    commitTxn();
  }

}