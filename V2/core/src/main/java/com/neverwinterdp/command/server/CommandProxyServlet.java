package com.neverwinterdp.command.server;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import javax.servlet.AsyncContext;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.proxy.ProxyServlet;
import org.eclipse.jetty.util.Callback;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.zk.RegistryImpl;

@SuppressWarnings("serial")
public class CommandProxyServlet extends ProxyServlet {

  private String forwardingUrl;
  private Registry registry;
  private String registryPathToCommandHost;
  
  
  @Override
  public void init() throws ServletException {
    super.init();
    ServletConfig conf = this.getServletConfig();
    RegistryConfig regConf = new RegistryConfig();
    
    //Get config from web.xml
    registryPathToCommandHost = conf.getInitParameter("registryPathToCommandHost"); 
    regConf.setConnect(conf.getInitParameter("registryhost"));
    regConf.setDbDomain(conf.getInitParameter("dbdomain"));
    registry = new RegistryImpl(regConf);
    try {
      registry.connect();
    } catch (RegistryException e) {
      e.printStackTrace();
    }
    this.setForwardingUrl();
  }
  
  @Override
  protected URI rewriteURI(HttpServletRequest request) {
    return URI.create(this.forwardingUrl);
  }
  
  
  @Override
  protected void onResponseFailure(HttpServletRequest request, HttpServletResponse response, 
                                    Response proxyResponse, Throwable failure){
    //System.err.println("Response Failure!");
    this.setForwardingUrl();
    
    HttpClient c = null;
    try {
      c = this.createHttpClient();
    } catch (ServletException e1) {
      e1.printStackTrace();
    }
    
    final Request proxyRequest =  c.newRequest(this.forwardingUrl)
        .method(request.getMethod())
        .version(HttpVersion.fromString(request.getProtocol()));
    
    boolean hasContent = request.getContentLength() > 0 || request.getContentType() != null;
    for (Enumeration<String> headerNames = request.getHeaderNames(); headerNames.hasMoreElements();){
        String headerName = headerNames.nextElement();
        if (HttpHeader.TRANSFER_ENCODING.is(headerName))
            hasContent = true;
        for (Enumeration<String> headerValues = request.getHeaders(headerName); headerValues.hasMoreElements();){
            String headerValue = headerValues.nextElement();
            if (headerValue != null)
                proxyRequest.header(headerName, headerValue);
        }
    }

    // Add proxy headers
    addViaHeader(proxyRequest);
    addXForwardedHeaders(proxyRequest, request);

    final AsyncContext asyncContext = request.getAsyncContext();
    // We do not timeout the continuation, but the proxy request
    asyncContext.setTimeout(0);
    proxyRequest.timeout(getTimeout(), TimeUnit.MILLISECONDS);

    if (hasContent)
      try {
        proxyRequest.content(proxyRequestContent(proxyRequest, request));
      } catch (IOException e) {
        e.printStackTrace();
      }

    customizeProxyRequest(proxyRequest, request);

    proxyRequest.send(new ProxyResponseListener(request, response));
  }
  
  
  private void setForwardingUrl(){
    //System.err.println("SettingForwardingUrl");
    try {
      this.forwardingUrl = new String(this.registry.getData(this.registryPathToCommandHost));
      //System.err.println(this.forwardingUrl);
    } catch (RegistryException e) {
      e.printStackTrace();
    }
  }
  
  /**
   * Shamelessly stolen from the parent class because they were cruel and made it private
   */
  private class ProxyResponseListener extends Response.Listener.Adapter
  {
      private final HttpServletRequest request;
      private final HttpServletResponse response;

      public ProxyResponseListener(HttpServletRequest request, HttpServletResponse response){
          this.request = request;
          this.response = response;
      }

      @Override
      public void onBegin(Response proxyResponse){
          response.setStatus(proxyResponse.getStatus());
      }

      @Override
      public void onHeaders(Response proxyResponse){
          onResponseHeaders(request, response, proxyResponse);

          if (_log.isDebugEnabled())
          {
              StringBuilder builder = new StringBuilder("\r\n");
              builder.append(request.getProtocol()).append(" ").append(response.getStatus()).append(" ").append(proxyResponse.getReason()).append("\r\n");
              for (String headerName : response.getHeaderNames())
              {
                  builder.append(headerName).append(": ");
                  for (Iterator<String> headerValues = response.getHeaders(headerName).iterator(); headerValues.hasNext();)
                  {
                      String headerValue = headerValues.next();
                      if (headerValue != null)
                          builder.append(headerValue);
                      if (headerValues.hasNext())
                          builder.append(",");
                  }
                  builder.append("\r\n");
              }
              _log.debug("{} proxying to downstream:{}{}{}{}{}",
                      getRequestId(request),
                      System.lineSeparator(),
                      proxyResponse,
                      System.lineSeparator(),
                      proxyResponse.getHeaders().toString().trim(),
                      System.lineSeparator(),
                      builder);
          }
      }

      @Override
      public void onContent(final Response proxyResponse, ByteBuffer content, final Callback callback){
          byte[] buffer;
          int offset;
          int length = content.remaining();
          if (content.hasArray())
          {
              buffer = content.array();
              offset = content.arrayOffset();
          }
          else
          {
              buffer = new byte[length];
              content.get(buffer);
              offset = 0;
          }

          onResponseContent(request, response, proxyResponse, buffer, offset, length, new Callback(){
              @Override
              public void succeeded()
              {
                  callback.succeeded();
              }

              @Override
              public void failed(Throwable x)
              {
                  callback.failed(x);
                  proxyResponse.abort(x);
              }
          });
      }

      @Override
      public void onComplete(Result result){
          if (result.isSucceeded())
              onResponseSuccess(request, response, result.getResponse());
          else
              onResponseFailure(request, response, result.getResponse(), result.getFailure());
          _log.debug("{} proxying complete", getRequestId(request));
      }
  }
  
  
}


