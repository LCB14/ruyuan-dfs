package com.ruyuan.dfs.namenode.server.tomcat.servlet;

import javax.servlet.*;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * 解决跨域Filter
 *
 * @author Sun Dasheng
 */
public class CorsFilter implements Filter {
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse servletResponse, FilterChain chain) throws IOException, ServletException {
        HttpServletResponse response = (HttpServletResponse) servletResponse;
        response.addHeader("Access-Control-Allow-Origin", "*");
        response.addHeader("Access-Control-Allow-Credentials", "true");
        response.addHeader("Access-Control-Allow-Methods", "POST,GET,OPTIONS,DELETE,PUT,PATCH,HEAD");
        response.addHeader("Access-Control-Allow-Headers", "*");
        response.addHeader("Access-Control-Max-Age", "3600");
        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {

    }
}
