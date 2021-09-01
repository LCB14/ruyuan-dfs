package com.ruyuan.dfs.namenode.server.tomcat.servlet;


import com.alibaba.fastjson.JSONObject;
import com.ruyuan.dfs.common.exception.ComponentNotFoundException;
import com.ruyuan.dfs.namenode.server.tomcat.VariablePathParser;
import com.ruyuan.dfs.namenode.server.tomcat.annotation.*;
import io.netty.handler.codec.http.HttpMethod;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.jar.JarInputStream;
import java.util.zip.ZipEntry;

/**
 * 请求分发Servlet
 *
 * @author Sun Dasheng
 */
@Slf4j
public class DispatcherServlet extends HttpServlet {

    private static final String BASE_PACKAGE = "com.ruyuan.dfs.namenode.server.tomcat";
    private List<String> classNames = new CopyOnWriteArrayList<>();
    private Map<String, Object> beanNameToInstanceMap = new ConcurrentHashMap<>();
    private Map<String, UrlMapping> urlToMethodMap = new ConcurrentHashMap<>();
    private Map<String, String> classNameToBeanNameMap = new ConcurrentHashMap<>();
    private VariablePathParser variablePathParser = new VariablePathParser();

    public DispatcherServlet() {
        try {
            // step1: 扫描包中所有的类，收集类的全限定名
            scanPackage(BASE_PACKAGE);
            // step2: 实例化Controller
            instantiateController();
            // step3: 注入Controller所需要的组件
            injectControllerComponent();
            // step4: 扫描所有Controller的接口，创建http接口映射关系
            handleUrlMethodMapping();
        } catch (Exception e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doGet(req, resp);
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doGet(req, resp);
    }

    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doGet(req, resp);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        try {
            String uri = trimUrl(req.getRequestURI());
            String method = req.getMethod();
            UrlMapping mapping = findMapping(uri, method);
            if (mapping == null) {
                sendError(resp, 404, "不支持的请求方式和路径：" + req.getMethod() + " " + uri);
                return;
            }
            Method invokeMethod = mapping.getInvokeMethod();
            String className = invokeMethod.getDeclaringClass().getCanonicalName();
            String beanName = classNameToBeanNameMap.get(className);
            Object bean = beanNameToInstanceMap.get(beanName);
            Object[] args = generateParameter(mapping, req);
            Object result = invokeMethod.invoke(bean, args);
            String response = JSONObject.toJSONString(result);
            resp.setCharacterEncoding("UTF-8");
            resp.setHeader("Content-Type", "application/json");
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.setContentType("application/json;charset=UTF-8");
            resp.getWriter().write(response);
            resp.getWriter().flush();
        } catch (Exception e) {
            String msg = e.getMessage();
            Throwable cause = e.getCause();
            while (cause != null) {
                msg = cause.getMessage();
                cause = cause.getCause();
            }
            sendError(resp, 500, "请求异常：" + msg);
            log.error("请求异常：", e);
        }
    }

    private void sendError(HttpServletResponse resp, int code, String msg) throws IOException {
        resp.setCharacterEncoding("UTF-8");
        resp.setHeader("Content-Type", "application/json");
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setContentType("application/json;charset=UTF-8");
        JSONObject object = new JSONObject();
        object.put("code", code);
        object.put("msg", msg);
        resp.getWriter().write(object.toJSONString());
        resp.getWriter().flush();
    }


    /**
     * 生成反射调用参数
     *
     * @param mapping 请求映射关系
     * @param request 请求
     * @return 调用参数
     * @throws Exception 异常
     */
    private Object[] generateParameter(UrlMapping mapping, HttpServletRequest request) throws Exception {
        if (mapping.getParameterList().isEmpty()) {
            return new Object[0];
        }
        Map<String, String> pathVariableMap = null;
        List<UrlMapping.ParamMetadata> parameterList = mapping.getParameterList();
        Object[] params = new Object[parameterList.size()];
        for (int i = 0; i < parameterList.size(); i++) {
            UrlMapping.ParamMetadata metadata = parameterList.get(i);
            if (metadata.getType().equals(UrlMapping.Type.PATH_VARIABLE)) {
                if (pathVariableMap == null) {
                    pathVariableMap = variablePathParser.extractVariable(trimUrl(request.getRequestURI()));
                }
                String pathVariableValue = pathVariableMap.get(metadata.getParamKey());
                Class<?> classType = metadata.getParamClassType();
                params[i] = mapValue(classType, pathVariableValue);
            } else if (metadata.getType().equals(UrlMapping.Type.REQUEST_BODY)) {
                if (request.getMethod().equals(HttpMethod.GET.toString())) {
                    throw new IllegalArgumentException("@RequestBody注解不支持GET请求方式");
                }
                if (!request.getContentType().contains("application/json")) {
                    throw new IllegalArgumentException("@RequestBody注解只支持json格式数据");
                }
                String json = readInput(request.getInputStream());
                try {
                    params[i] = JSONObject.parseObject(json, metadata.getParamClassType());
                } catch (Exception e) {
                    throw new IllegalArgumentException("JSON格式有误： " + json);
                }
            } else if (metadata.getType().equals(UrlMapping.Type.QUERY_ENTITY)) {
                Map<String, String> queriesMap = extractQueries(request);
                Class<?> paramClassType = metadata.getParamClassType();
                Field[] declaredFields = paramClassType.getDeclaredFields();
                Object param = paramClassType.newInstance();
                for (Field field : declaredFields) {
                    String name = field.getName();
                    String value = queriesMap.get(name);
                    if (value == null) {
                        continue;
                    }
                    field.setAccessible(true);
                    field.set(param, mapValue(field.getType(), value));
                }
                params[i] = param;
            }
        }
        return params;
    }

    private String readInput(InputStream inputStream) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        String temp;
        while ((temp = br.readLine()) != null) {
            sb.append(temp);
        }
        br.close();
        return sb.toString();
    }

    private UrlMapping findMapping(String uri, String method) {
        UrlMapping mapping = urlToMethodMap.get(uri + "#" + method);
        if (mapping != null) {
            return mapping;
        }
        String urlWithVariable = variablePathParser.match(uri);
        if (urlWithVariable != null) {
            mapping = urlToMethodMap.get(urlWithVariable + "#" + method);
        }
        return mapping;
    }


    /**
     * 建立URL映射关系
     *
     * @throws ClassNotFoundException 无法找到Class
     */
    private void handleUrlMethodMapping() throws ClassNotFoundException {
        for (String className : classNames) {
            Class<?> clazz = Class.forName(className);
            if (!clazz.isAnnotationPresent(RestController.class)) {
                continue;
            }
            String baseUrl = "";
            if (clazz.isAnnotationPresent(RequestMapping.class)) {
                RequestMapping annotation = clazz.getAnnotation(RequestMapping.class);
                baseUrl = annotation.value();
            }
            Method[] methods = clazz.getMethods();
            for (Method m : methods) {
                if (!m.isAnnotationPresent(RequestMapping.class)) {
                    continue;
                }
                RequestMapping annotation = m.getAnnotation(RequestMapping.class);
                String path = annotation.value();
                String fullPath = trimUrl(baseUrl + path);
                if (variablePathParser.containVariable(path)) {
                    variablePathParser.add(fullPath);
                    if (log.isDebugEnabled()) {
                        log.debug("保存路径变量映射：[path={}]", fullPath);
                    }
                }
                UrlMapping mapping = new UrlMapping();
                String method = annotation.method();
                mapping.setUrl(fullPath);
                mapping.setMethod(method);
                mapping.setInvokeMethod(m);
                parseParameterMetadata(mapping, m.getParameters());
                // 保存url对controller method 的映射
                if (log.isDebugEnabled()) {
                    log.debug("保存URL映射：[header={}, method={}]", method + " " + fullPath, className + "#" + m.getName());
                }
                String key = fullPath + "#" + method;
                if (urlToMethodMap.containsKey(key)) {
                    throw new IllegalArgumentException("出现重复的接口声明：" + key);
                }
                urlToMethodMap.put(key, mapping);
            }
        }
    }

    private void parseParameterMetadata(UrlMapping mapping, Parameter[] parameters) {
        boolean hasRequestBody = false;
        boolean hasQueries = false;
        String name = mapping.getInvokeMethod().getDeclaringClass().getSimpleName() + "#" + mapping.getInvokeMethod().getName();
        for (Parameter parameter : parameters) {
            if (parameter.isAnnotationPresent(PathVariable.class)) {
                PathVariable annotation = parameter.getAnnotation(PathVariable.class);
                String variableKey = annotation.value();
                mapping.addParameterList(UrlMapping.Type.PATH_VARIABLE, variableKey, parameter.getType());
            } else if (parameter.isAnnotationPresent(RequestBody.class)) {
                if (hasRequestBody) {
                    throw new IllegalArgumentException("@RequestBody注解在同一个接口只支持一个: " + name);
                }
                hasRequestBody = true;
                Class<?> type = parameter.getType();
                mapping.addParameterList(UrlMapping.Type.REQUEST_BODY, null, type);
            } else {
                if (hasQueries) {
                    throw new IllegalArgumentException("GET请求实体只支持一个：" + name);
                }
                hasQueries = true;
                Class<?> type = parameter.getType();
                mapping.addParameterList(UrlMapping.Type.QUERY_ENTITY, null, type);
            }
        }
    }

    private void injectControllerComponent() throws Exception {
        for (Map.Entry<String, Object> entry : beanNameToInstanceMap.entrySet()) {
            Field[] fields = entry.getValue().getClass().getDeclaredFields();
            for (Field field : fields) {
                if (!field.isAnnotationPresent(Autowired.class)) {
                    continue;
                }
                Autowired annotation = field.getAnnotation(Autowired.class);
                String needInjectBeanName = annotation.value();
                if (needInjectBeanName.length() == 0) {
                    needInjectBeanName = field.getType().getSimpleName();
                }
                field.setAccessible(true);
                Object component = DispatchComponentProvider.getInstance().getComponent(needInjectBeanName);
                if (component == null) {
                    throw new ComponentNotFoundException("Can not find component " + needInjectBeanName);
                }
                field.set(entry.getValue(), component);
                if (log.isDebugEnabled()) {
                    log.debug("注入Controller组件：[{}] ", entry.getValue().getClass().getSimpleName() + "#" + field.getName());
                }
            }
        }
    }

    private void instantiateController() throws Exception {
        for (String className : classNames) {
            Class<?> clazz = Class.forName(className);
            if (!clazz.isAnnotationPresent(RestController.class)) {
                continue;
            }
            RestController annotation = clazz.getAnnotation(RestController.class);
            String beanName = annotation.value();
            if ("".equals(beanName)) {
                beanName = clazz.getSimpleName();
            }
            beanNameToInstanceMap.put(beanName, clazz.newInstance());
            classNameToBeanNameMap.put(className, beanName);
            if (log.isDebugEnabled()) {
                log.debug("注册RestController: [name={}]", beanName);
            }
        }
    }

    private void scanPackage(String packageName) throws IOException {
        String basePackage = packageName.replaceAll("\\.", "/");
        URL url = this.getClass().getClassLoader().getResource(basePackage);
        if (log.isDebugEnabled()) {
            log.debug("scanClass Url={}", url);
        }
        String rootPath = getRootPath(url);
        if (rootPath.endsWith("jar")) {
            readFromJarFile(rootPath, basePackage);
        } else {
            File basePackageFile = new File(URLDecoder.decode(url.getPath(), StandardCharsets.UTF_8.name()));
            File[] children = basePackageFile.listFiles();
            if (children == null) {
                return;
            }
            for (File file : children) {
                if (file.isDirectory()) {
                    scanPackage(packageName + "." + file.getName());
                } else {
                    // 扫描出来的类是.class后缀名的，需要去掉
                    String clazz = packageName + "." + file.getName().split("\\.")[0];
                    if (log.isDebugEnabled()) {
                        log.debug("扫描到类：[class={}]", clazz);
                    }
                    classNames.add(clazz);
                }
            }
        }
    }

    private void readFromJarFile(String jar, String basePackage) throws IOException {
        JarInputStream jarIn = new JarInputStream(new FileInputStream(jar));
        ZipEntry nextEntry = jarIn.getNextEntry();
        while (nextEntry != null) {
            String name = nextEntry.getName();
            if (name.startsWith(basePackage) && name.endsWith(".class")) {
                name = name.substring(0, name.indexOf(".")).replaceAll("/", ".");
                if (log.isDebugEnabled()) {
                    log.debug("扫描到类：[class={}]", name);
                }
                classNames.add(name);
            }
            nextEntry = jarIn.getNextJarEntry();
        }
    }

    /**
     * file:/home/xxx => /home/xxx
     * jar:file:/home/xxx.jar!com/ruyuan => /home/xxx.jar
     */
    private String getRootPath(URL url) {
        String file = url.getFile();
        int pos = file.indexOf("!");
        if (pos == -1) {
            return file;
        } else {
            return file.substring(5, pos);
        }
    }


    private Map<String, String> extractQueries(HttpServletRequest request) {
        if (request.getQueryString() == null) {
            return new HashMap<>(2);
        }
        StringTokenizer st = new StringTokenizer(request.getQueryString(), "&");
        int i;
        Map<String, String> queries = new HashMap<>(2);
        while (st.hasMoreTokens()) {
            String s = st.nextToken();
            i = s.indexOf("=");
            if (i > 0 && s.length() >= i + 1) {
                String name = s.substring(0, i);
                String value = s.substring(i + 1);
                try {
                    name = URLDecoder.decode(name, "UTF-8");
                    value = URLDecoder.decode(value, "UTF-8");
                } catch (Exception e) {
                    e.printStackTrace();
                }
                queries.put(name, value);
            } else if (i == -1) {
                String name = s;
                String value = "";
                try {
                    name = URLDecoder.decode(name, "UTF-8");
                } catch (Exception e) {
                    e.printStackTrace();
                }
                queries.put(name, value);
            }
        }
        return queries;
    }

    private String trimUrl(String uri) {
        uri = uri.replaceAll("//", "/");
        if (uri.endsWith("/")) {
            uri = uri.substring(0, uri.length() - 1);
        }
        return uri;
    }

    /**
     * 将参数转化为具体的类型
     *
     * @param classType 参数类型
     * @param value     参数值
     * @return 转换后的结果
     */
    private Object mapValue(Class<?> classType, String value) {
        if (classType.equals(String.class)) {
            return value;
        } else if (classType.equals(Integer.class)) {
            return Integer.parseInt(value);
        } else if (classType.equals(Long.class)) {
            return Long.parseLong(value);
        }
        return value;
    }
}
