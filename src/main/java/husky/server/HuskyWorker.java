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

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.util.Records;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

abstract class ContainerRunnable implements Runnable {
  private static final Logger LOG = Logger.getLogger(ContainerRunnable.class.getName());
  protected HuskyApplicationMaster mAppMaster = null;
  protected HuskyRMCallbackHandler mRMCallbackHandler = null;
  protected Container mContainer = null;

  public ContainerRunnable(HuskyApplicationMaster appMaster, HuskyRMCallbackHandler rmCallbackHandler, Container container) {
    LOG.info("New container " + container.getId() + " starts on " + container.getNodeId().getHost());
    mContainer = container;
    mAppMaster = appMaster;
    mRMCallbackHandler = rmCallbackHandler;
  }

  public String getHost() {
    return mContainer.getNodeId().getHost();
  }

  protected Map<String, String> getShellEnv() {
    Map<String, String> map = new HashMap<String, String>();
    if (!mAppMaster.getLdLibraryPath().isEmpty()) {
      map.put("LD_LIBRARY_PATH", mAppMaster.getLdLibraryPath());
    }
    return map;
  }

  protected abstract List<String> getCommands() throws Exception;

  public void run() {
    try {
      ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
      ctx.setLocalResources(mRMCallbackHandler.getLocalResources());
      ctx.setEnvironment(getShellEnv());
      ctx.setCommands(getCommands());
      mAppMaster.getNMClient().startContainerAsync(mContainer, ctx);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to start worker container on " + getHost(), e);
    }
  }

}

class HuskyWorker extends ContainerRunnable {
  private static final Logger LOG = Logger.getLogger(HuskyWorker.class.getName());

  public HuskyWorker(HuskyApplicationMaster appMaster, HuskyRMCallbackHandler rmCallbackHandler, Container container) {
    super(appMaster, rmCallbackHandler, container);
  }

  protected List<String> getCommands() throws InterruptedException {
    ArrayList<String> commands = new ArrayList<String>();
    StringBuilder builder = new StringBuilder();
    builder.append("./HuskyAppExec")
        .append(" --conf HuskyConfigFile")
        .append(" --master_host ").append(mAppMaster.getAppMasterHost())
        .append(" --worker.info");
    for (Pair<String, Integer> info : mAppMaster.getWorkerInfos()) {
      builder.append(" ").append(info.getFirst()).append(':').append(info.getSecond());
    }

    String containerHost = mContainer.getNodeId().getHost();
    builder.append(" 1>").append("<LOG_DIR>/").append(containerHost).append(".stdout");
    builder.append(" 2>").append("<LOG_DIR>/").append(containerHost).append(".stderr");
    if (!mAppMaster.getLogPathToHDFS().isEmpty()) {
      builder.append("; worker_exit_code=$?")
          .append("; hadoop fs -put -f <LOG_DIR>/")
          .append(containerHost).append(".stdout ")
          .append(mAppMaster.getLogPathToHDFS())
          .append("; hadoop fs -put -f <LOG_DIR>/")
          .append(containerHost).append(".stderr ")
          .append(mAppMaster.getLogPathToHDFS())
          .append("; exit \"$worker_exit_code\"");
    }

    LOG.info("Worker command: " + builder.toString());
    commands.add(builder.toString());
    return commands;
  }
}
