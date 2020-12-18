package cromwell.backend.google.pipelines.v2beta.api

import com.google.api.services.lifesciences.v2beta.model.{Action, Mount}
import cromwell.backend.google.pipelines.common.api.PipelinesApiRequestFactory.CreatePipelineParameters
import cromwell.backend.google.pipelines.v2beta.LifeSciencesFactory

trait CheckpointingAction {
  def checkpointingSetupActions(createPipelineParameters: CreatePipelineParameters,
                                mounts: List[Mount]
                            ): List[Action] =
    createPipelineParameters.runtimeAttributes.checkpointingAttributes map { checkpointing =>
      val checkpointingImage = LifeSciencesFactory.CloudSdkImage
      val checkpointingCommand = createPipelineParameters.checkpointingConfiguration.checkpointingCommand(checkpointing)
      val checkpointingEnvironment = Map.empty[String, String]


      val initialCheckpointSyncAction = ActionBuilder.cloudSdkShellAction(
        createPipelineParameters.checkpointingConfiguration.localizePreviousCheckpointCommand(checkpointing.file)
      )(mounts = mounts)
      val describeInitialCheckpointingSyncAction = ActionBuilder.describeDocker("initial checkpointing sync", initialCheckpointSyncAction)

      val backgroundCheckpointingAction = ActionBuilder.backgroundAction(
        checkpointingImage,
        checkpointingCommand,
        checkpointingEnvironment,
        mounts
      )
      val describeBackgroundCheckpointingAction = ActionBuilder.describeDocker("begin checkpointing background action", backgroundCheckpointingAction)

      List(describeInitialCheckpointingSyncAction, initialCheckpointSyncAction, describeBackgroundCheckpointingAction, backgroundCheckpointingAction)
    } getOrElse(Nil)

  def checkpointingShutdownActions(createPipelineParameters: CreatePipelineParameters): List[Action] = {
    createPipelineParameters.runtimeAttributes.checkpointingAttributes map { checkpointing =>
      val terminationAction = ActionBuilder.terminateBackgroundActionsAction()
      val describeTerminationAction = ActionBuilder.describeDocker("terminate checkpointing action", terminationAction)

      val deleteCheckpointAction = ActionBuilder.gcsFileDeletionAction(createPipelineParameters.checkpointingConfiguration.checkpointFileCloud(checkpointing.file))
      val describeDeleteCheckpointAction = ActionBuilder.describeDocker("remove checkpointing file", deleteCheckpointAction)

      List(describeTerminationAction, terminationAction, describeDeleteCheckpointAction, deleteCheckpointAction)
    } getOrElse(Nil)
  }
}
