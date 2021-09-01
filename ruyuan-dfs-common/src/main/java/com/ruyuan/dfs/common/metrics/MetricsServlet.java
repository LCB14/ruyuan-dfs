package com.ruyuan.dfs.common.metrics;

import com.alibaba.fastjson.JSONObject;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.*;

/**
 * MetricsServlet，提供监控信息
 *
 * @author Sun Dasheng
 */
public class MetricsServlet extends HttpServlet {

    private CollectorRegistry registry;

    public MetricsServlet() {
        registry = CollectorRegistry.defaultRegistry;
    }

    @Override
    protected void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws IOException {
        resp.setStatus(200);
        String isCloudPlatform = req.getParameter("isCloudPlatform");
        if ("true".equals(isCloudPlatform)) {
            resp.setContentType("application/json; charset=utf8");
            Enumeration<Collector.MetricFamilySamples> enumeration = registry.metricFamilySamples();
            List<Collector.MetricFamilySamples> ret = new ArrayList<>();
            while (enumeration.hasMoreElements()) {
                Collector.MetricFamilySamples samples = enumeration.nextElement();
                ret.add(samples);
            }
            resp.getWriter().write(JSONObject.toJSONString(ret));
        } else {
            String contentType = TextFormat.chooseContentType(req.getHeader("Accept"));
            resp.setContentType(contentType);
            try (Writer writer = new BufferedWriter(resp.getWriter())) {
                TextFormat.writeFormat(contentType, writer, registry.filteredMetricFamilySamples(parse(req)));
                writer.flush();
            }
        }
    }


    private Set<String> parse(HttpServletRequest req) {
        String[] includedParam = req.getParameterValues("name[]");
        if (includedParam == null) {
            return Collections.emptySet();
        } else {
            return new HashSet<>(Arrays.asList(includedParam));
        }
    }

    @Override
    protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws IOException {
        doGet(req, resp);
    }
}
