<%@page import="com.google.appengine.api.memcache.MemcacheServiceFactory"%>
<%@page import="com.google.appengine.api.memcache.MemcacheService"%>
<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>

<%
    MemcacheService memcacheService = MemcacheServiceFactory.getMemcacheService();
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
	<head>
	   <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
	   <title>PDF Builder results</title>
	</head>
	<body>
		<h1>Processing invoices</h1>
		
		<p>When the invoices are generated <a href="/download?id=<%= request.getParameter("id") %>">this link</a> can be used
		to download the zip file.</p>
        		
		<a href="/index.jsp">Back to the upload form</a>
	</body>
</html>