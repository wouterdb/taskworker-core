<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<%@ page import="com.google.appengine.api.blobstore.BlobstoreServiceFactory" %>
<%@ page import="com.google.appengine.api.blobstore.BlobstoreService" %>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>

<%
    BlobstoreService blobstoreService = BlobstoreServiceFactory.getBlobstoreService();
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
	<head>
	   <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
	   <title>Workflow</title>
	</head>
	<body>
		<h1>Workflow interface</h1>
		<div id="error" style="color: red">${error}</div>
		<div id="info" style="color: green">${info}</div>
		
        <form action="<%= blobstoreService.createUploadUrl("/start") %>" method="POST" enctype="multipart/form-data">
            <label for="workflow">Workflow:</label>
		    <select name="workflow" id="workflow">
                <c:forEach var="workflow" items="${workflows}">
                <option value="${workflow}">${workflow}</option>
                </c:forEach>
            </select><br />
		
			<label for="file">File input:</label>
			<input type="file" name="file" id="file" /><br />
			
			<label for="text">Data input:</label>
			<textarea name="text" id="text"></textarea><br />
			
			<input type="submit" name="submit" value="Submit">
		</form>
	</body>
</html>