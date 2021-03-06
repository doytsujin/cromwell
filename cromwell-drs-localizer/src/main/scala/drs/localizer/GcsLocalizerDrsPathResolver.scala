package drs.localizer

import cloud.nio.impl.drs.{DrsConfig, DrsPathResolver}
import com.google.auth.oauth2.GoogleCredentials

import scala.collection.JavaConverters._


class GcsLocalizerDrsPathResolver(drsConfig: DrsConfig)
  extends DrsPathResolver(drsConfig) {

  private final val UserInfoEmailScope = "https://www.googleapis.com/auth/userinfo.email"
  private final val UserInfoProfileScope = "https://www.googleapis.com/auth/userinfo.profile"
  private final val UserInfoScopes = List(UserInfoEmailScope, UserInfoProfileScope)

  override def getAccessToken: String = {
    val scopedCredentials = GoogleCredentials.getApplicationDefault().createScoped(UserInfoScopes.asJava)
    scopedCredentials.refreshAccessToken().getTokenValue
  }
}
