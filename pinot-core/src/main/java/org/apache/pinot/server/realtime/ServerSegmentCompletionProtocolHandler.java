/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.server.realtime;

import java.io.File;
import java.net.URI;
import java.util.Map;
import javax.net.ssl.SSLContext;
import org.apache.commons.configuration.Configuration;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.ClientSSLContextGenerator;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.core.query.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.common.utils.CommonConstants.Server.SegmentCompletionProtocol.*;


/**
 * A class that handles sending segment completion protocol requests to the controller and getting
 * back responses
 */
public class ServerSegmentCompletionProtocolHandler {
  private static Logger LOGGER = LoggerFactory.getLogger(ServerSegmentCompletionProtocolHandler.class);
  private static final String HTTPS_PROTOCOL = "https";
  private static final String HTTP_PROTOCOL = "http";

  private static SSLContext _sslContext;
  private static Integer _controllerHttpsPort;
  private static int _segmentUploadRequestTimeoutMs;

  private final FileUploadDownloadClient _fileUploadDownloadClient;
  private final ServerMetrics _serverMetrics;
  private final String _rawTableName;

  public static void init(Configuration uploaderConfig) {
    Configuration httpsConfig = uploaderConfig.subset(HTTPS_PROTOCOL);
    if (httpsConfig.getBoolean(CONFIG_OF_CONTROLLER_HTTPS_ENABLED, false)) {
      _sslContext = new ClientSSLContextGenerator(httpsConfig.subset(CommonConstants.PREFIX_OF_SSL_SUBSET)).generate();
      _controllerHttpsPort = httpsConfig.getInt(CONFIG_OF_CONTROLLER_HTTPS_PORT);
    }
    _segmentUploadRequestTimeoutMs =
        uploaderConfig.getInt(CONFIG_OF_SEGMENT_UPLOAD_REQUEST_TIMEOUT_MS, DEFAULT_SEGMENT_UPLOAD_REQUEST_TIMEOUT_MS);
  }

  public ServerSegmentCompletionProtocolHandler(ServerMetrics serverMetrics, String tableNameWithType) {
    _fileUploadDownloadClient = new FileUploadDownloadClient(_sslContext);
    _serverMetrics = serverMetrics;
    _rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
  }

  public SegmentCompletionProtocol.Response segmentCommitStart(SegmentCompletionProtocol.Request.Params params) {
    SegmentCompletionProtocol.SegmentCommitStartRequest request =
        new SegmentCompletionProtocol.SegmentCommitStartRequest(params);
    String url = createSegmentCompletionUrl(request);
    if (url == null) {
      return SegmentCompletionProtocol.RESP_NOT_SENT;
    }
    return sendRequest(url);
  }

  // TODO We need to make this work with trusted certificates if the VIP is using https.
  public SegmentCompletionProtocol.Response segmentCommitUpload(SegmentCompletionProtocol.Request.Params params,
      final File segmentTarFile, final String controllerVipUrl) {
    SegmentCompletionProtocol.SegmentCommitUploadRequest request =
        new SegmentCompletionProtocol.SegmentCommitUploadRequest(params);

    String hostPort;
    String protocol;
    try {
      URI uri = URI.create(controllerVipUrl);
      protocol = uri.getScheme();
      hostPort = uri.getAuthority();
    } catch (Exception e) {
      throw new RuntimeException("Could not make URI", e);
    }
    String url = request.getUrl(hostPort, protocol);
    return uploadSegment(url, params.getSegmentName(), segmentTarFile);
  }

  // Replaced by segmentCommitEndWithMetadata().
  @Deprecated
  public SegmentCompletionProtocol.Response segmentCommitEnd(SegmentCompletionProtocol.Request.Params params) {
    SegmentCompletionProtocol.SegmentCommitEndRequest request =
        new SegmentCompletionProtocol.SegmentCommitEndRequest(params);
    String url = createSegmentCompletionUrl(request);
    if (url == null) {
      return SegmentCompletionProtocol.RESP_NOT_SENT;
    }
    return sendRequest(url);
  }

  public SegmentCompletionProtocol.Response segmentCommitEndWithMetadata(
      SegmentCompletionProtocol.Request.Params params, final Map<String, File> metadataFiles) {
    SegmentCompletionProtocol.SegmentCommitEndWithMetadataRequest request =
        new SegmentCompletionProtocol.SegmentCommitEndWithMetadataRequest(params);
    String url = createSegmentCompletionUrl(request);
    if (url == null) {
      return SegmentCompletionProtocol.RESP_NOT_SENT;
    }
    return sendCommitEndWithMetadataFiles(url, metadataFiles);
  }

  public SegmentCompletionProtocol.Response segmentCommit(SegmentCompletionProtocol.Request.Params params,
      final File segmentTarFile) {
    SegmentCompletionProtocol.SegmentCommitRequest request = new SegmentCompletionProtocol.SegmentCommitRequest(params);
    String url = createSegmentCompletionUrl(request);
    if (url == null) {
      return SegmentCompletionProtocol.RESP_NOT_SENT;
    }

    return uploadSegment(url, params.getSegmentName(), segmentTarFile);
  }

  public SegmentCompletionProtocol.Response extendBuildTime(SegmentCompletionProtocol.Request.Params params) {
    SegmentCompletionProtocol.ExtendBuildTimeRequest request =
        new SegmentCompletionProtocol.ExtendBuildTimeRequest(params);
    String url = createSegmentCompletionUrl(request);
    return sendRequest(url);
  }

  public SegmentCompletionProtocol.Response segmentConsumed(SegmentCompletionProtocol.Request.Params params) {
    SegmentCompletionProtocol.SegmentConsumedRequest request =
        new SegmentCompletionProtocol.SegmentConsumedRequest(params);
    String url = createSegmentCompletionUrl(request);
    if (url == null) {
      return SegmentCompletionProtocol.RESP_NOT_SENT;
    }
    return sendRequest(url);
  }

  public SegmentCompletionProtocol.Response segmentStoppedConsuming(SegmentCompletionProtocol.Request.Params params) {
    SegmentCompletionProtocol.SegmentStoppedConsuming request =
        new SegmentCompletionProtocol.SegmentStoppedConsuming(params);
    String url = createSegmentCompletionUrl(request);
    if (url == null) {
      return SegmentCompletionProtocol.RESP_NOT_SENT;
    }
    return sendRequest(url);
  }

  private String createSegmentCompletionUrl(SegmentCompletionProtocol.Request request) {
    ControllerLeaderLocator leaderLocator = ControllerLeaderLocator.getInstance();
    final Pair<String, Integer> leaderHostPort = leaderLocator.getControllerLeader(_rawTableName);
    if (leaderHostPort == null) {
      LOGGER.warn("No leader found while trying to send {}", request.toString());
      return null;
    }
    String protocol = HTTP_PROTOCOL;
    if (_controllerHttpsPort != null) {
      leaderHostPort.setSecond(_controllerHttpsPort);
      protocol = HTTPS_PROTOCOL;
    }

    return request.getUrl(leaderHostPort.getFirst() + ":" + leaderHostPort.getSecond(), protocol);
  }

  private SegmentCompletionProtocol.Response sendRequest(String url) {
    SegmentCompletionProtocol.Response response;
    try {
      String responseStr =
          _fileUploadDownloadClient.sendSegmentCompletionProtocolRequest(new URI(url), DEFAULT_OTHER_REQUESTS_TIMEOUT)
              .getResponse();
      response = SegmentCompletionProtocol.Response.fromJsonString(responseStr);
      LOGGER.info("Controller response {} for {}", response.toJsonString(), url);
      if (response.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.NOT_LEADER)) {
        ControllerLeaderLocator.getInstance().invalidateCachedControllerLeader();
      }
    } catch (Exception e) {
      // Catch all exceptions, we want the protocol to handle the case assuming the request was never sent.
      response = SegmentCompletionProtocol.RESP_NOT_SENT;
      LOGGER.error("Could not send request {}", url, e);
      // Invalidate controller leader cache, as exception could be because of leader being down (deployment/failure) and hence unable to send {@link SegmentCompletionProtocol.ControllerResponseStatus.NOT_LEADER}
      // If cache is not invalidated, we will not recover from exceptions until the controller comes back up
      ControllerLeaderLocator.getInstance().invalidateCachedControllerLeader();
    }
    raiseSegmentCompletionProtocolResponseMetric(response);
    return response;
  }

  private SegmentCompletionProtocol.Response sendCommitEndWithMetadataFiles(String url,
      Map<String, File> metadataFiles) {
    SegmentCompletionProtocol.Response response;
    try {
      String responseStr = _fileUploadDownloadClient
          .uploadSegmentMetadataFiles(new URI(url), metadataFiles, _segmentUploadRequestTimeoutMs).getResponse();
      response = SegmentCompletionProtocol.Response.fromJsonString(responseStr);
      LOGGER.info("Controller response {} for {}", response.toJsonString(), url);
      if (response.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.NOT_LEADER)) {
        ControllerLeaderLocator.getInstance().invalidateCachedControllerLeader();
      }
    } catch (Exception e) {
      // Catch all exceptions, we want the protocol to handle the case assuming the request was never sent.
      response = SegmentCompletionProtocol.RESP_NOT_SENT;
      LOGGER.error("Could not send request {}", url, e);
      // Invalidate controller leader cache, as exception could be because of leader being down (deployment/failure) and hence unable to send {@link SegmentCompletionProtocol.ControllerResponseStatus.NOT_LEADER}
      // If cache is not invalidated, we will not recover from exceptions until the controller comes back up
      ControllerLeaderLocator.getInstance().invalidateCachedControllerLeader();
    }
    raiseSegmentCompletionProtocolResponseMetric(response);
    return response;
  }

  private SegmentCompletionProtocol.Response uploadSegment(String url, final String segmentName,
      final File segmentTarFile) {
    SegmentCompletionProtocol.Response response;
    try {
      String responseStr = _fileUploadDownloadClient
          .uploadSegment(new URI(url), segmentName, segmentTarFile, null, null, _segmentUploadRequestTimeoutMs)
          .getResponse();
      response = SegmentCompletionProtocol.Response.fromJsonString(responseStr);
      LOGGER.info("Controller response {} for {}", response.toJsonString(), url);
      if (response.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.NOT_LEADER)) {
        ControllerLeaderLocator.getInstance().invalidateCachedControllerLeader();
      }
    } catch (Exception e) {
      // Catch all exceptions, we want the protocol to handle the case assuming the request was never sent.
      response = SegmentCompletionProtocol.RESP_NOT_SENT;
      LOGGER.error("Could not send request {}", url, e);
      // Invalidate controller leader cache, as exception could be because of leader being down (deployment/failure) and hence unable to send {@link SegmentCompletionProtocol.ControllerResponseStatus.NOT_LEADER}
      // If cache is not invalidated, we will not recover from exceptions until the controller comes back up
      ControllerLeaderLocator.getInstance().invalidateCachedControllerLeader();
    }
    raiseSegmentCompletionProtocolResponseMetric(response);
    return response;
  }

  /**
   * raise a metric indicating the response we received from the controller
   *
   * @param response
   */
  private void raiseSegmentCompletionProtocolResponseMetric(SegmentCompletionProtocol.Response response) {
    switch (response.getStatus()) {
      case NOT_SENT:
        _serverMetrics.addMeteredGlobalValue(ServerMeter.LLC_CONTROLLER_RESPONSE_NOT_SENT, 1);
        break;
      case COMMIT:
        _serverMetrics.addMeteredGlobalValue(ServerMeter.LLC_CONTROLLER_RESPONSE_COMMIT, 1);
        break;
      case HOLD:
        _serverMetrics.addMeteredGlobalValue(ServerMeter.LLC_CONTROLLER_RESPONSE_HOLD, 1);
        break;
      case CATCH_UP:
        _serverMetrics.addMeteredGlobalValue(ServerMeter.LLC_CONTROLLER_RESPONSE_CATCH_UP, 1);
        break;
      case DISCARD:
        _serverMetrics.addMeteredGlobalValue(ServerMeter.LLC_CONTROLLER_RESPONSE_DISCARD, 1);
        break;
      case KEEP:
        _serverMetrics.addMeteredGlobalValue(ServerMeter.LLC_CONTROLLER_RESPONSE_KEEP, 1);
        break;
      case NOT_LEADER:
        _serverMetrics.addMeteredGlobalValue(ServerMeter.LLC_CONTROLLER_RESPONSE_NOT_LEADER, 1);
        break;
      case FAILED:
        _serverMetrics.addMeteredGlobalValue(ServerMeter.LLC_CONTROLLER_RESPONSE_FAILED, 1);
        break;
      case COMMIT_SUCCESS:
        _serverMetrics.addMeteredGlobalValue(ServerMeter.LLC_CONTROLLER_RESPONSE_COMMIT_SUCCESS, 1);
        break;
      case COMMIT_CONTINUE:
        _serverMetrics.addMeteredGlobalValue(ServerMeter.LLC_CONTROLLER_RESPONSE_COMMIT_CONTINUE, 1);
        break;
      case PROCESSED:
        _serverMetrics.addMeteredGlobalValue(ServerMeter.LLC_CONTROLLER_RESPONSE_PROCESSED, 1);
        break;
      case UPLOAD_SUCCESS:
        _serverMetrics.addMeteredGlobalValue(ServerMeter.LLC_CONTROLLER_RESPONSE_UPLOAD_SUCCESS, 1);
        break;
    }
  }
}
