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
<%@page import="java.util.Comparator"%>
<%@page import="java.util.TreeSet"%>
<%@page import="java.util.SortedSet"%>
<%@page import="drm.taskworker.monitoring.Statistic"%>
<%@page import="java.util.Set"%>
<%@page import="java.util.Map"%>
<%@page import="drm.taskworker.tasks.AbstractTask"%>
<%@page import="java.util.List"%>
<%@page import="drm.taskworker.tasks.Task"%>
<%@page import="drm.taskworker.monitoring.Monitor"%>
<%@page import="drm.taskworker.monitoring.IMonitor"%>
<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>

<%
    IMonitor mon = new Monitor();
    
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
	<head>
	   <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
	   <title>Monitor</title>
	   <style type="text/css">
	   td{
	    padding-left: 10px;
	    padding-right: 10px;
	   }
	   </style>
	</head>
	<body>
		<table>
		<tr> <td>role</td><td>name</td> <td> cost/s </td>  <td> avg time</td>   <td> stdev </td> <td> samples count </td> </tr>
		<% for(Map.Entry<String,Set<Statistic>> e:mon.getStats().entrySet()) {
			SortedSet<Statistic> ss = new TreeSet(new Comparator<Statistic>(){public int compare(Statistic o1, Statistic o2){return o1.getName().compareTo(o2.getName());}});
			ss.addAll(e.getValue());
			%>
			
		<tr> 
		<td  colspan="6"> <%=e.getKey()%></td>
	    </tr>
	    <%for(Statistic s:ss){ %>
	    <tr> 
		<td> <%=s.getRole()%></td><td> <%=s.getName()%></td> <td> <%=s.getCost()%></td>  <td> <%=String.format("%.4f ms",s.getAverage()*1000)%></td>   <td> <%=String.format("%.4f ms",s.getSdtDev()*1000)%></td> <td> <%=s.getSamples()%></td>
	    </tr>
	    <% } %>
	    <tr>
	    
	    <tr>
	    <% } %>
	    </table>
	</body>
</html>