package cromwell.backend.google.pipelines.v2alpha1.api

import com.google.api.services.genomics.v2alpha1.model.{Action, Mount}
import cromwell.backend.google.pipelines.common.api.PipelinesApiRequestFactory.CreatePipelineParameters
import cromwell.backend.google.pipelines.v2alpha1.GenomicsFactory

trait CheckpointingAction {
  def checkpointingSetupActions(createPipelineParameters: CreatePipelineParameters,
                                mounts: List[Mount]
                            ): List[Action] =
    createPipelineParameters.runtimeAttributes.checkpointingAttributes map { checkpointing =>

      // Initial sync from cloud:
      val initialCheckpointSyncAction = ActionBuilder.cloudSdkShellAction(
        createPipelineParameters.checkpointingConfiguration.localizePreviousCheckpointCommand(checkpointing.file)
      )(mounts = mounts)
      val describeInitialCheckpointingSyncAction = ActionBuilder.describeDocker("initial checkpointing sync", initialCheckpointSyncAction)

      // Background upload action:
      val backgroundCheckpointingAction = ActionBuilder.backgroundAction(
        image = GenomicsFactory.CloudSdkImage,
        command = createPipelineParameters.checkpointingConfiguration.checkpointingCommand(checkpointing, ActionCommands.multiLineCommand),
        environment = Map.empty[String, String],
        mounts = mounts
      )
      val describeBackgroundCheckpointingAction = ActionBuilder.describeDocker("begin checkpointing background action", backgroundCheckpointingAction)

      List(describeInitialCheckpointingSyncAction, initialCheckpointSyncAction, describeBackgroundCheckpointingAction, backgroundCheckpointingAction)
    } getOrElse(Nil)

  def checkpointingShutdownActions(createPipelineParameters: CreatePipelineParameters): List[Action] =
    createPipelineParameters.runtimeAttributes.checkpointingAttributes map { checkpointing =>
      val terminationAction = ActionBuilder.terminateBackgroundActionsAction()
      val describeTerminationAction = ActionBuilder.describeDocker("terminate checkpointing action", terminationAction)

      val deleteCheckpointAction = ActionBuilder.gcsFileDeletionAction(createPipelineParameters.checkpointingConfiguration.checkpointFileCloud(checkpointing.file))
      val describeDeleteCheckpointAction = ActionBuilder.describeDocker("remove checkpointing file", deleteCheckpointAction)

      List(describeTerminationAction, terminationAction, describeDeleteCheckpointAction, deleteCheckpointAction)
    } getOrElse(Nil)
}
