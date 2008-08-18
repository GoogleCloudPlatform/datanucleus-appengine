// Copyright 2008 Google Inc. All Rights Reserved.
package com.google.appengine.helloorm;

import javax.jdo.PersistenceManager;
import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class UpdateFlight extends HttpServlet {

  private ServletConfig config;
  public void init(ServletConfig config) {
    this.config = config;
  }

  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String key = req.getParameter("key");
    String orig = req.getParameter("orig");
    String dest = req.getParameter("dest");
    if (key == null) {
      resp.getWriter().println("No key provided.");
      return;
    }

    if (orig == null) {
      resp.getWriter().println("No origin provided.");
      return;
    }

    if (dest == null) {
      resp.getWriter().println("No destination provided.");
      return;
    }
    if (PersistenceStandard.get() == PersistenceStandard.JPA) {
      EntityManager em = EMF.emf.createEntityManager();
      try {
        EntityTransaction txn = em.getTransaction();
        txn.begin();
        Flight f = em.find(Flight.class, key);
        f.setOrig(orig);
        f.setDest(dest);
        txn.commit();
      } finally {
        em.close();
      }
    } else {
      PersistenceManager pm = PMF.pmf.getPersistenceManager();
      try {
        Flight f = pm.getObjectById(Flight.class, key);
        pm.currentTransaction().begin();
        f.setOrig(orig);
        f.setDest(dest);
        pm.currentTransaction().commit();
      } finally {
        pm.close();
      }
    }
    resp.sendRedirect("/");
  }

}
