package com.smart.servlet;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import com.smart.cache.CacheTemplate;
import com.smart.util.Jsons;
import com.smart.util.Strings;
import com.smart.util.Utils;

public class CacheAdminServlet extends HttpServlet {

    private static final long  serialVersionUID    = 4033915519705451079L;

    public static final String SESSION_USER_KEY    = "smartcache";
    public static final String PARAM_NAME_USERNAME = "username";
    public static final String PARAM_NAME_PASSWORD = "password";
    public static final String RESOURCE_PATH       = "support";

    public final static int    RESULT_CODE_SUCCESS = 1;
    public final static int    RESULT_CODE_FALIURE = -1;

    protected String           username            = null;
    protected String           password            = null;

    @Override
    public void init() throws ServletException {
        String paramUserName = getInitParameter(PARAM_NAME_USERNAME);
        if (!Strings.isEmpty(paramUserName)) {
            this.username = paramUserName;
        }
        String paramPassword = getInitParameter(PARAM_NAME_PASSWORD);
        if (!Strings.isEmpty(paramPassword)) {
            this.password = paramPassword;
        }
    }

    public void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String contextPath = request.getContextPath();
        String servletPath = request.getServletPath();
        String requestURI = request.getRequestURI();

        response.setCharacterEncoding("utf-8");

        if (contextPath == null) { // root context
            contextPath = "";
        }
        String uri = contextPath + servletPath;
        String path = requestURI.substring(contextPath.length() + servletPath.length());

        if ("/submitLogin".equals(path)) {
            String usernameParam = request.getParameter(PARAM_NAME_USERNAME);
            String passwordParam = request.getParameter(PARAM_NAME_PASSWORD);
            if (username.equals(usernameParam) && password.equals(passwordParam)) {
                request.getSession().setAttribute(SESSION_USER_KEY, username);
                response.getWriter().print("success");
            } else {
                response.getWriter().print("error");
            }
            return;
        }
        if (isRequireAuth() //
                && !isContainsUser(request)//
                && !checkLoginParam(request)//
                && !("/login.html".equals(path) || path.startsWith("/static"))) {
            if (contextPath.equals("") || contextPath.equals("/")) {
                response.sendRedirect("/smartcache/login.html");
            } else {
                if ("".equals(path)) {
                    response.sendRedirect("smartcache/login.html");
                } else {
                    response.sendRedirect("login.html");
                }
            }
            return;
        }
        if ("".equals(path)) {
            if (contextPath.equals("") || contextPath.equals("/")) {
                response.sendRedirect("/smartcache/admin.html");
            } else {
                response.sendRedirect("smartcache/admin.html");
            }
            return;
        }
        if ("/".equals(path)) {
            response.sendRedirect("admin.html");
            return;
        }

        if (path.contains(".json")) {
            String fullUrl = path;
            if (request.getQueryString() != null && request.getQueryString().length() > 0) {
                fullUrl += "?" + request.getQueryString();
            }
            response.getWriter().print(process(fullUrl, request));
            return;
        }

        // find file in resources path
        returnResourceFile(path, uri, response);
    }

    protected String process(String url, HttpServletRequest request) {
        Map<String, String> parameters = getParameters(url);

        WebApplicationContext context = WebApplicationContextUtils.getWebApplicationContext(request.getSession().getServletContext());

        CacheTemplate cacheTemplate = context.getBean(CacheTemplate.class);
        String name = parameters.get("name");
        String key = parameters.get("key");
        url = Strings.subString(url, null, ".json");
        if (url.equals("/get")) {
            if ((null != name && name.length() > 0) && (null != key && key.length() > 0)) {
                return returnJSONResultSuccess(RESULT_CODE_SUCCESS, cacheTemplate.get(name, key));
            }
        }
        //
        else if (url.equals("/del")) {
            if ((null != name && name.length() > 0) && (null != key && key.length() > 0)) {
                cacheTemplate.del(name, key);
                return returnJSONResultSuccess(RESULT_CODE_SUCCESS, null);
            }
        }
        //
        else if (url.equals("/rem")) {
            if (null != name && name.length() > 0) {
                cacheTemplate.rem(name);
                return returnJSONResultSuccess(RESULT_CODE_SUCCESS, null);
            }
        }
        //
        else if (url.equals("/names")) {
            return returnJSONResultSuccess(RESULT_CODE_SUCCESS, cacheTemplate.names());
        }
        //
        else if (url.equals("/keys")) {
            if (null != name && name.length() > 0) {
                return returnJSONResultSuccess(RESULT_CODE_SUCCESS, cacheTemplate.keys(name));
            }
        }
        //
        else if (url.equals("/fetch")) {
            if ((null != name && name.length() > 0) && (null != key && key.length() > 0)) {
                return returnJSONResultSuccess(RESULT_CODE_SUCCESS, cacheTemplate.fetch(name, key));
            }
        }
        //
        else if (url.equals("/cls")) {
            cacheTemplate.cls();
            return returnJSONResultSuccess(RESULT_CODE_SUCCESS, null);
        }
        //
        return returnJSONResultFailure(RESULT_CODE_FALIURE, "Do not support this request, please contact with administrator.");

    }

    protected void returnResourceFile(String fileName, String uri, HttpServletResponse response) throws ServletException, IOException {

        String filePath = getFilePath(fileName);

        if (filePath.endsWith(".html")) {
            response.setContentType("text/html; charset=utf-8");
        }
        if (fileName.endsWith(".jpg")) {
            byte[] bytes = Utils.readByteArrayFromResource(filePath);
            if (bytes != null) {
                response.getOutputStream().write(bytes);
            }

            return;
        }

        String text = Utils.readFromResource(filePath);
        if (text == null) {
            response.sendRedirect(uri + "/admin.html");
            return;
        }
        if (fileName.endsWith(".css")) {
            response.setContentType("text/css;charset=utf-8");
        } else if (fileName.endsWith(".js")) {
            response.setContentType("text/javascript;charset=utf-8");
        }
        response.getWriter().write(text);
    }

    public static Map<String, String> getParameters(String url) {
        if (url == null || (url = url.trim()).length() == 0) {
            return Collections.<String, String> emptyMap();
        }
        String parametersStr = Strings.subString(url, "?", null);
        if (parametersStr == null || parametersStr.length() == 0) {
            return Collections.<String, String> emptyMap();
        }

        String[] parametersArray = parametersStr.split("&");
        Map<String, String> parameters = new LinkedHashMap<String, String>();

        for (String parameterStr : parametersArray) {
            int index = parameterStr.indexOf("=");
            if (index <= 0) {
                continue;
            }

            String name = parameterStr.substring(0, index);
            String value = parameterStr.substring(index + 1);
            parameters.put(name, value);
        }
        return parameters;
    }

    public boolean isRequireAuth() {
        return this.username != null;
    }

    public boolean isContainsUser(HttpServletRequest request) {
        HttpSession session = request.getSession(false);
        return session != null && session.getAttribute(SESSION_USER_KEY) != null;
    }

    public boolean checkLoginParam(HttpServletRequest request) {
        String usernameParam = request.getParameter(PARAM_NAME_USERNAME);
        String passwordParam = request.getParameter(PARAM_NAME_PASSWORD);
        if (null == username || null == password) {
            return false;
        } else if (username.equals(usernameParam) && password.equals(passwordParam)) {
            return true;
        }
        return false;
    }

    protected String getFilePath(String fileName) {
        return RESOURCE_PATH + fileName;
    }

    public static String returnJSONResult(int code, Object data, String msg) {
        Map<String, Object> dataMap = new LinkedHashMap<String, Object>();
        dataMap.put("code", code);
        dataMap.put("data", data);
        dataMap.put("msg", msg);
        return Jsons.toJSONString(dataMap);
    }

    public static String returnJSONResultSuccess(int code, Object data) {
        return returnJSONResult(code, data, null);
    }

    public static String returnJSONResultFailure(int code, String msg) {
        return returnJSONResult(code, null, msg);
    }

}
