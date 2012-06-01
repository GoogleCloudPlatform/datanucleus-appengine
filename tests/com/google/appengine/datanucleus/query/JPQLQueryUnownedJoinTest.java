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

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.datanucleus.jpa.JPATestCase;
import com.google.appengine.datanucleus.test.jpa.UnownedJoinsJPA.Course;
import com.google.appengine.datanucleus.test.jpa.UnownedJoinsJPA.Major;
import com.google.appengine.datanucleus.test.jpa.UnownedJoinsJPA.Student;

import static com.google.appengine.datanucleus.test.jpa.UnownedJoinsJPA.newCourse;
import static com.google.appengine.datanucleus.test.jpa.UnownedJoinsJPA.newMajor;
import static com.google.appengine.datanucleus.test.jpa.UnownedJoinsJPA.newStudent;

import java.util.Collections;

import javax.persistence.PersistenceException;
import javax.persistence.Query;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPQLQueryUnownedJoinTest extends JPATestCase {

  public void testTransientMeansTransient() throws EntityNotFoundException {
    Student student = new Student();
    beginTxn();
    em.persist(student);
    commitTxn();
    Entity studentEntity = ds.get(KeyFactory.createKey(kindForClass(Student.class), student.getId()));
    assertEquals(3, studentEntity.getProperties().size());
    assertTrue(studentEntity.hasProperty("courses"));
    assertTrue(studentEntity.hasProperty("grade"));
    assertTrue(studentEntity.hasProperty("major"));
  }

  public void testJoinOnOneToMany_Simple() {
    Course course1 = newCourse("Biology");
    Course course2 = newCourse("Not Biology");
    persistInTxn(course1, course2);
    Student student = newStudent(10, course1, course2);
    beginTxn();
    em.persist(student);
    commitTxn();
    beginTxn();
    Query q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.coursesAlias c where "
        + "c.department = 'Biology' and "
        + "s.grade = 10");
    
    assertEquals(student.getId(), ((Student) q.getSingleResult()).getId());
    commitTxn();
  }

  public void testJoinOnOneToMany_LegalOrderBy() {
    Course course1 = newCourse("Biology");
    Course course2 = newCourse("Not Biology");
    persistInTxn(course1, course2);
    Student student = newStudent(10, course1, course2);
    persistInTxn(student);
    beginTxn();
    Query q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.coursesAlias c where "
        + "c.department = 'Biology' and "
        + "s.grade = 10 order by s.courses asc");
    assertEquals(student.getId(), ((Student) q.getSingleResult()).getId());
    commitTxn();
  }

  private void persistInTxn(Object... objects) {
    for (Object o : objects) {
      beginTxn();
      em.persist(o);
      commitTxn();
    }
  }

  public void testJoinOnOneToMany_Offset() {
    Course course1 = newCourse("Biology");
    Course course2 = newCourse("Not Biology");
    Course course3 = newCourse("Biology");
    Course course4 = newCourse("Not Biology");
    Course course5 = newCourse("Biology");
    Course course6 = newCourse("Not Biology");
    persistInTxn(course1, course2, course3, course4, course5, course6);
    Student student = newStudent(10, course1, course2);
    Student student2 = newStudent(11, course3, course4);
    Student student3 = newStudent(10, course5, course6);
    persistInTxn(student, student2, student3);
    beginTxn();
    Query q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.coursesAlias c where "
        + "c.department = 'Biology' and "
        + "s.grade = 10");
    q.setFirstResult(1);
    assertEquals(student3.getId(), ((Student) q.getSingleResult()).getId());
    q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.coursesAlias c where "
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
    persistInTxn(course1, course2, course3, course4, course5, course6);
    Student student = newStudent(10, course1, course2);
    Student student2 = newStudent(11, course3, course4);
    Student student3 = newStudent(10, course5, course6);
    persistInTxn(student, student2, student3);
    beginTxn();
    Query q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.coursesAlias c where "
        + "c.department = 'Biology' and "
        + "s.grade = 10");
    q.setMaxResults(1);
    assertEquals(student.getId(), ((Student) q.getSingleResult()).getId());
    q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.coursesAlias c where "
        + "c.department = 'Biology' and "
        + "s.grade = 10");
    q.setMaxResults(0);
    assertEquals(Collections.emptyList(), q.getResultList());
    commitTxn();
  }

  public void testJoinOnOneToOne_Simple() {
    Major major1 = newMajor("Liberal Arts");
    Major major2 = newMajor("Engineering");
    persistInTxn(major1, major2);
    Student student1 = newStudent(10, major1);
    Student student2 = newStudent(10, major2);
    persistInTxn(student1, student2);
    beginTxn();
    Query q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.majorAlias m where "
        + "m.school = 'Liberal Arts' and "
        + "s.grade = 10");
    assertEquals(student1.getId(), ((Student) q.getSingleResult()).getId());
    commitTxn();
  }

  public void testJoinOnOneToMany_Illegal() {
    beginTxn();
    // all filters on parent must be equality filters
    Query q = em.createQuery(
        "select from " + Student.class.getName() + " s JOIN s.coursesAlias c where "
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
        "select from " + Student.class.getName() + " s JOIN s.coursesAlias c where "
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
        "select from " + Student.class.getName() + " s JOIN s.coursesAlias c where "
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
        "select from " + Student.class.getName() + " s JOIN s.coursesAlias c where "
        + "c.department = 'Biology' and "
        + "s.grade = 10 order by s.coursesAlias desc");
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
        "select from " + Student.class.getName() + " s JOIN s.coursesAlias c where "
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