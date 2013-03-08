<%--


        Copyright 2013 KU Leuven Research and Development - iMinds - Distrinet

        Licensed under the Apache License, Version 2.0 (the "License");
        you may not use this file except in compliance with the License.
        You may obtain a copy of the License at

            http://www.apache.org/licenses/LICENSE-2.0

        Unless required by applicable law or agreed to in writing, software
        distributed under the License is distributed on an "AS IS" BASIS,
        WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
        See the License for the specific language governing permissions and
        limitations under the License.

        Administrative Contact: dnet-project-office@cs.kuleuven.be
        Technical Contact: bart.vanbrabant@cs.kuleuven.be

--%>
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
			
			<label for="date">Start when (number of seconds from now):</label>
            <input type="text" name="date" id="date" value="5" /><br />
			
			<input type="submit" name="submit" value="Submit">
		</form>
		<p><a href="/index.jsp">Back to start</a></p>
	</body>
</html>