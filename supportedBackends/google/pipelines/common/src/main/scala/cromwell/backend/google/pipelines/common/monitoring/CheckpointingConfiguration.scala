package cromwell.backend.google.pipelines.common.monitoring

import cromwell.backend.BackendJobDescriptor
import cromwell.backend.io.WorkflowPaths
import cromwell.core.path.Path

final class CheckpointingConfiguration(jobDescriptor: BackendJobDescriptor,
                                       workflowPaths: WorkflowPaths,
                                       commandDirectory: Path
                                      ) {

  def checkpointFileCloud(checkpointFileName: String): String = {
    // Fix the attempt at 1 because we always use the base directory to store the checkpoint file.
    workflowPaths.toJobPaths(jobDescriptor.key.copy(attempt = 1), jobDescriptor.workflowDescriptor)
      .callExecutionRoot.resolve(checkpointFileName).toAbsolutePath.pathAsString
  }

  def checkpointFileLocal(checkpointFileName: String): String = {
    commandDirectory.resolve(checkpointFileName).toAbsolutePath.pathAsString
  }

  def localizePreviousCheckpointCommand(checkpointFileName: String): List[String] = {
    val local = checkpointFileLocal(checkpointFileName)
    val cloud = checkpointFileCloud(checkpointFileName)
    List(
      "/bin/sh",
      "-c",
      s"echo 'Syncing from checkpoint $cloud' && " +
        s"gsutil cp $cloud $local || touch $local"
    )
  }

  def checkpointingCommand(checkpointFileName: String): List[String] = {
    val local = checkpointFileLocal(checkpointFileName)
    val cloud = checkpointFileCloud(checkpointFileName)
    List(
      "/bin/sh",
      "-xc",
      s"touch $local && " +
        s"while true; do " +
        s"gsutil -m cp $local $cloud-tmp && " +
        s"gsutil -m mv $cloud-tmp $cloud && " +
        "sleep 600; done"
    )
  }
}
