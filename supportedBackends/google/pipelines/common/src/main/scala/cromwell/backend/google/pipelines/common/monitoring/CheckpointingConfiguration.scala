package cromwell.backend.google.pipelines.common.monitoring

import cromwell.backend.BackendJobDescriptor
import cromwell.backend.google.pipelines.common.PipelinesApiRuntimeAttributes
import cromwell.backend.io.WorkflowPaths
import cromwell.core.path.Path

final class CheckpointingConfiguration(jobDescriptor: BackendJobDescriptor,
                                       workflowPaths: WorkflowPaths,
                                       commandDirectory: Path,
                                       runtimeAttributes: PipelinesApiRuntimeAttributes
                                      ) {

  lazy val checkpointFileCloud: Option[String] = runtimeAttributes.checkpointFile map { ckpt =>
    // Fix the attempt at 1 because we always use the base directory to store the checkpoint file.
    workflowPaths.toJobPaths(jobDescriptor.key.copy(attempt = 1), jobDescriptor.workflowDescriptor)
      .callExecutionRoot.resolve(ckpt).toAbsolutePath.pathAsString
  }

  lazy val checkpointFileLocal: Option[String] = runtimeAttributes.checkpointFile map { ckpt =>
    commandDirectory.resolve(ckpt).toAbsolutePath.pathAsString
  }

  val localizePreviousCheckpointCommand: List[String] = (for {
    local <- checkpointFileLocal
    cloud <- checkpointFileCloud
  } yield List(
    "/bin/sh",
    "-c",
    s"echo 'Syncing from checkpoint $cloud' && " +
      s"gsutil cp $cloud $local || touch $local"
  )).getOrElse(List.empty)


  val checkpointingCommand: List[String] = (for {
    local <- checkpointFileLocal
    cloud <- checkpointFileCloud
  } yield List(
    "/bin/sh",
    "-xc",
    s"touch $local && " +
      s"while true; do ls -lh $local &&" +
      s"gsutil -m cp $local $cloud-tmp && " +
      s"gsutil -m mv $cloud-tmp $cloud && " +
      "sleep 10; done"
  )).getOrElse(List.empty)
}
