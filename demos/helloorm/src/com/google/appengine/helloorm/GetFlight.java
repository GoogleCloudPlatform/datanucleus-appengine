// Copyright 2008 Google Inc. All Rights Reserved.
package com.google.appengine.helloorm;

import javax.jdo.PersistenceManager;
import javax.persistence.EntityManager;
import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class GetFlight extends HttpServlet {

  private ServletConfig config;
  @Override
  public void init(ServletConfig config) {
    this.config = config;
  }

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String key = req.getParameter("key");
    if (key == null) {
      resp.getWriter().println("No key provided.");
      return;
    }
    Flight f;
    PersistenceManager pm = null;
    EntityManager em = null;
    try {
      if (PersistenceStandard.get() == PersistenceStandard.JPA) {
        em = EMF.emf.createEntityManager();
        f = em.find(Flight.class, key);
      } else {
        pm = PMF.pmf.getPersistenceManager();
        f = pm.getObjectById(Flight.class, key);
      }
      resp.getWriter().println("<form action=\"updateFlight\" method=\"post\">");
      resp.getWriter().println("<input name=\"key\" type=\"hidden\" value=\"" + key + "\"/>");
      resp.getWriter().println("<table>");
      resp.getWriter().println("<tr>");
      resp.getWriter().println("<th>Origin</th><td><input name=\"orig\" type=\"text\" value=\"" + f.getOrig() + "\"/></td>");
      resp.getWriter().println("<th>Destination</th><td><input name=\"dest\" type=\"text\" value=\"" + f.getDest() + "\"/></td>");
      resp.getWriter().println("</tr>");
      resp.getWriter().println("<tr><td><input type=\"submit\" value=\"Update Flight\"></td></tr>");
      resp.getWriter().println("</table>");
      resp.getWriter().println("</form>");
      resp.getWriter().println("<form action=\"deleteFlight\" method=\"post\">");
      resp.getWriter().println("<input name=\"key\" type=\"hidden\" value=\"" + key + "\"/>");
      resp.getWriter().println("<input type=\"submit\" value=\"Delete Flight\"></td></tr>");
      resp.getWriter().println("</form>");
    } finally {
      if (pm != null) {
        pm.close();
      }

      if (em != null) {
        em.close();
      }
    }
  }
}
