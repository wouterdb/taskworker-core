<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
	<head>
	   <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
	   <title>Workflow list</title>
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
		      <dl>
        <c:forEach var="task" items="${workflow.history}">
          <dd>${task.getId()} - ${task.getParentTask().getId()}</dd>
        </c:forEach>
        </dl>
        
        <table>
            <tr>
            </tr>
            
        </table>
        </c:if>
	</body>
</html>
