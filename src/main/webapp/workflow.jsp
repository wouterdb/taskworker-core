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
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
	<head>
	   <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
	   <title>Workflow list</title>
	   
	   <script type="text/javascript" src="js/raphael-min.js"></script>
       <script type="text/javascript" src="js/graffle.js"></script>
       <script type="text/javascript" src="js/graph.js"></script>
	</head>
	<body>
		<h1>Workflow list</h1>
        <form action="/workflow" method="GET">
            <select name="workflowId">
                <c:forEach var="workflow" items="${workflows}">
				<option value="${workflow.workflowId}">${workflow.workflowId} - ${workflow.name}</option>
			    </c:forEach>
			</select>
			
            <input type="submit" name="submit" value="Submit">
		</form>
		
		<c:if test="${not empty workflow}">
		<h2>Workflow history for ${workflow.workflowId} - ${workflow.name}</h2>
		<p>
		  Started at ${started }<br />
		  Finished at ${finished }
		</p>
        <table>
            <tr>
                <th>Worker type</th>
                <th>Task processed</th>
            </tr>
            <c:forEach items="${stats}" var="entry">
            <tr>
                <td>${entry.key }</td>
                <td>${entry.value }</td>
            </tr>
            </c:forEach>
        </table>
        </c:if>
        <p><a href="/download?id=workflow-${workflow.workflowId}">Download Result</a></p>
        <p><a href="/index.jsp">Back to start</a></p>
	</body>
</html>
