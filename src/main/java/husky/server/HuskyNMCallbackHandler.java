/* Copyright 2016 Husky Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package husky.server;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync.CallbackHandler;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HuskyNMCallbackHandler implements CallbackHandler {
  static private final Logger LOG = Logger.getLogger(HuskyNMCallbackHandler.class.getName());

  public HuskyNMCallbackHandler() {
  }

  public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
    LOG.info("Container with id " + containerId + " starts.");
  }

  public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
    // TODO Auto-generated method stub
  }

  public void onContainerStopped(ContainerId containerId) {
    LOG.info("Container with id " + containerId + " is stopped.");
  }

  public void onStartContainerError(ContainerId containerId, Throwable t) {
    LOG.log(Level.SEVERE, "Error thrown while starting container with id " + containerId, t);
  }

  public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
    // TODO Auto-generated method stub
  }

  public void onStopContainerError(ContainerId containerId, Throwable t) {
    // TODO Auto-generated method stub
  }

}
