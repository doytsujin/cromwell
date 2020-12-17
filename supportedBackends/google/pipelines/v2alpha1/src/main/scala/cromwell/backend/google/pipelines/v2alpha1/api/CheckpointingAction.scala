package cromwell.backend.google.pipelines.v2alpha1.api

import com.google.api.services.genomics.v2alpha1.model.{Action, Mount}
import cromwell.backend.google.pipelines.common.api.PipelinesApiRequestFactory.CreatePipelineParameters
import cromwell.backend.google.pipelines.v2alpha1.GenomicsFactory
import scala.collection.JavaConverters._

trait CheckpointingAction {
  def checkpointingSetupActions(createPipelineParameters: CreatePipelineParameters,
                                mounts: List[Mount]
                            ): List[Action] =
    createPipelineParameters.runtimeAttributes.checkpointFile map { checkpointFile =>
      val checkpointingImage = GenomicsFactory.CloudSdkImage
      val checkpointingCommand = createPipelineParameters.checkpointingConfiguration.checkpointingCommand(checkpointFile)
      val checkpointingEnvironment = Map.empty[String, String]

      val initialCheckpointSyncAction = ActionBuilder.monitoringAction(
        checkpointingImage,
        createPipelineParameters.checkpointingConfiguration.localizePreviousCheckpointCommand(checkpointFile),
        checkpointingEnvironment,
        mounts
      ).setFlags(List.empty[String].asJava)
      val describeInitialCheckpointingSyncAction = ActionBuilder.describeDocker("initial checkpointing sync", initialCheckpointSyncAction)

      val backgroundCheckpointingAction = ActionBuilder.monitoringAction(
        checkpointingImage,
        checkpointingCommand,
        checkpointingEnvironment,
        mounts
      )
      val describeBackgroundCheckpointingAction = ActionBuilder.describeDocker("begin checkpointing background action", backgroundCheckpointingAction)

      List(describeInitialCheckpointingSyncAction, initialCheckpointSyncAction, describeBackgroundCheckpointingAction, backgroundCheckpointingAction)
    } getOrElse(List.empty)

  def checkpointingShutdownActions(createPipelineParameters: CreatePipelineParameters): List[Action] =
    createPipelineParameters.runtimeAttributes.checkpointFile match {
      case Some(_) =>
        val terminationAction = ActionBuilder.monitoringTerminationAction()

        val describeTerminationAction = ActionBuilder.describeDocker("terminate checkpointing action", terminationAction)

        List(describeTerminationAction, terminationAction)
      case None => Nil
    }
}
