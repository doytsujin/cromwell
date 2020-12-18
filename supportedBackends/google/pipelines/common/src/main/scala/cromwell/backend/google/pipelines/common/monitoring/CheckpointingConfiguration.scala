package cromwell.backend.google.pipelines.common.monitoring

import cromwell.backend.BackendJobDescriptor
import cromwell.backend.google.pipelines.common.CheckpointingAttributes
import cromwell.backend.io.WorkflowPaths
import cromwell.core.path.Path

final class CheckpointingConfiguration(jobDescriptor: BackendJobDescriptor,
                                       workflowPaths: WorkflowPaths,
                                       commandDirectory: Path
                                      ) {
  def checkpointFileCloud(checkpointFileName: String): String = {
    // Fix the attempt at 1 because we always use the base directory to store the checkpoint file.
    workflowPaths.toJobPaths(jobDescriptor.key.copy(attempt = 1), jobDescriptor.workflowDescriptor)
      .callExecutionRoot.resolve("__checkpointing").resolve(checkpointFileName).toAbsolutePath.pathAsString
  }

  def checkpointFileLocal(checkpointFileName: String): String = {
    commandDirectory.resolve(checkpointFileName).toAbsolutePath.pathAsString
  }

  def localizePreviousCheckpointCommand(checkpointFileName: String): String = {
    val local = checkpointFileLocal(checkpointFileName)
    val cloud = checkpointFileCloud(checkpointFileName)

    s"gsutil cp $cloud $local || touch $local"
  }

  def checkpointingCommand(checkpointingAttributes: CheckpointingAttributes, multilineActionSquasher: String => String): List[String] = {
    val local = checkpointFileLocal(checkpointingAttributes.file)
    val cloud = checkpointFileCloud(checkpointingAttributes.file)
    val checkpointUploadScript =
      s"""touch $local
         |while true
         |do
         |  # Attempt to make a local copy of the checkpoint file
         |  echo "CHECKPOINTING: Making a local copy of $local at $local-tmp"
         |  COPY_SUCCESS="false"
         |  while [ "$$COPY_SUCCESS" != "true" ]
         |  do
         |    PRE_COPY_TIMESTAMP="$$(stat -c'%Z' $local)"
         |    cp $local $local-tmp
         |    if [ "$$PRE_COPY_TIMESTAMP" == "$$(stat -c'%Z' $local)" ]
         |    then
         |      COPY_SUCCESS="true"
         |    else
         |      echo "CHECKPOINTING: $local was modified while trying to make a local copy. Will retry in 10s..."
         |      sleep 10
         |    fi
         |  done
         |
         |  # Perform the upload:
         |  echo "CHECKPOINTING: Uploading $local-tmp to $cloud-tmp"
         |  gsutil -m mv $local-tmp $cloud-tmp
         |  echo "CHECKPOINTING: Moving $cloud-tmp to be the new $cloud"
         |  gsutil -m mv $cloud-tmp $cloud
         |  echo "CHECKPOINTING: Sleeping for ${checkpointingAttributes.interval.toString} before next checkpoint"
         |  sleep ${checkpointingAttributes.interval.toSeconds}
         |done""".stripMargin

    List(
      "/bin/bash",
      "-c",
      multilineActionSquasher(checkpointUploadScript)
    )
  }
}
