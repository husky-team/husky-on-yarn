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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HuskyRMCallbackHandler implements AMRMClientAsync.CallbackHandler {
  private static final Logger LOG = Logger.getLogger(HuskyRMCallbackHandler.class.getName());

  private Thread masterThread = null;
  private ArrayList<Thread> mContainerThreads = new ArrayList<Thread>();
  private int mNumCompletedContainers = 0;
  private int mNumContainers = 0;
  private int mNumSuccess = 0;
  private HuskyApplicationMaster mAppMaster = null;

  class Lock {
    private boolean value = false;

    public void unlock() {
      value = true;
    }

    public boolean isLocked() {
      return value == false;
    }
  }

  private Lock finalResultLock = new Lock();

  public HuskyRMCallbackHandler(HuskyApplicationMaster master) {
    mAppMaster = master;
    mNumContainers = mAppMaster.getWorkerInfos().size();
    masterThread = new HuskyMaster(master);
    masterThread.start();
  }

  public float getProgress() {
    return ((float) mNumCompletedContainers) / mNumContainers;
  }

  public int getFinalNumSuccess() throws InterruptedException {
    if (finalResultLock.isLocked()) {
      synchronized (finalResultLock) {
        if (finalResultLock.isLocked()) {
          finalResultLock.wait();
        }
      }
    }
    return mNumSuccess;
  }

  public String getStatusReport() {
    return String.format("Allocated: %d, Completed: %d, Succeeded: %d, Failed: %d\n", mContainerThreads.size(),
        mNumCompletedContainers, mNumSuccess, mNumCompletedContainers - mNumSuccess);
  }

  public void onContainersAllocated(List<Container> allocatedContainers) {
    LOG.info("Get response from RM for container request, allocatedCnt = " + allocatedContainers.size());
    constructLocalResources();
    for (Container container : allocatedContainers) {
      ContainerRunnable runnable = new HuskyWorker(mAppMaster, this, container);
      Thread containerThread = new Thread(runnable);
      mContainerThreads.add(containerThread);
      containerThread.start();
    }
  }

  public void onContainersCompleted(List<ContainerStatus> completedContainerStatus) {
    LOG.info("Get response from RM for container request, completedCnt = " + completedContainerStatus.size());
    mNumCompletedContainers += completedContainerStatus.size();
    for (ContainerStatus status : completedContainerStatus) {
      LOG.info(String.format("Container %s: %s, exit status: %d", status.getContainerId().toString(),
          status.getState().toString(), status.getExitStatus()));
      if (status.getExitStatus() == 0) {
        mNumSuccess += 1;
      }
    }
    LOG.info("Total containers: " + mNumContainers + ", completed containers: " + mNumCompletedContainers);
    if (mNumContainers == mNumCompletedContainers) {
      // If all workers and master finish
      synchronized (finalResultLock) {
        finalResultLock.unlock();
        finalResultLock.notifyAll();
      }
    }
  }

  public void onError(Throwable arg0) {
    mAppMaster.getRMClient().stop();
  }

  public void onNodesUpdated(List<NodeReport> updatedNodes) {
  }

  public void onShutdownRequest() {
  }

  // should access through getLocalResources()
  private Map<String, LocalResource> localResources = null;

  private LocalResource constructLocalResource(String name, String path, LocalResourceType type) throws IOException {
    LOG.info("To copy " + name + " from " + path);
    if (!path.startsWith("hdfs://")) {
      throw new RuntimeException("Local resources provided to application master must be already located on HDFS.");
    }
    FileStatus dsfStaus = mAppMaster.getFileSystem().getFileStatus(new Path(path));
    LocalResource resource = Records.newRecord(LocalResource.class);
    resource.setType(type);
    resource.setVisibility(LocalResourceVisibility.APPLICATION);
    resource.setResource(ConverterUtils.getYarnUrlFromPath(new Path(path)));
    resource.setTimestamp(dsfStaus.getModificationTime());
    resource.setSize(dsfStaus.getLen());
    return resource;
  }

  private void constructLocalResources() {
    localResources = new HashMap<String, LocalResource>();

    try {
      String[][] resources = {{"HuskyAppExec", mAppMaster.getAppExec()},
          {"HuskyMasterExec", mAppMaster.getMasterExec()}, {"HuskyConfigFile", mAppMaster.getConfigFile()}};

      for (String[] resource : resources) {
        LocalResource lr = constructLocalResource(resource[0], resource[1], LocalResourceType.FILE);
        localResources.put(resource[0], lr);
      }

      for (String i : mAppMaster.getLocalFiles().split(",")) {
        i = i.trim();
        if (!i.isEmpty()) {
          String name = new Path(i).getName();
          LocalResource lr = constructLocalResource(name, i, LocalResourceType.FILE);
          localResources.put(name, lr);
        }
      }

      for (String i : mAppMaster.getLocalArchives().split(",")) {
        i = i.trim();
        if (!i.isEmpty()) {
          String name = new Path(i).getName();
          LocalResource lr = constructLocalResource(name, i, LocalResourceType.ARCHIVE);
          localResources.put(name, lr);
        }
      }
    } catch (IOException e) {
      LOG.log(Level.WARNING, " Failed to construct local resource map: ", e);
    }
  }

  Map<String, LocalResource> getLocalResources() {
    if (localResources == null) {
      constructLocalResources();
    }
    return localResources;
  }
}
