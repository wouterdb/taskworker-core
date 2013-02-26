package drm.demo;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;

/**
 * Servlet implementation class DownloadServlet
 */
public class DownloadServlet extends HttpServlet {
	private MemcacheService memcacheService = MemcacheServiceFactory.getMemcacheService();

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public DownloadServlet() {
		super();
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		String id = request.getParameter("id");

		if (memcacheService.contains(id)) {
			byte[] zipFile = (byte[]) memcacheService.get(id);

			// sets response content type
			response.setContentType("application/zip");
			response.setContentLength(zipFile.length);

			// sets HTTP header
			response.setHeader("Content-Disposition", "attachment; filename=\"invoices.zip\"");

			ServletOutputStream outStream = response.getOutputStream();
			outStream.write(zipFile, 0, zipFile.length);
			outStream.close();
		} else {
			response.sendRedirect("/invoices.jsp?id=" + id);
		}
	}

}
