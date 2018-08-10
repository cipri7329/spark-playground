package connectors.elasticsearch

import java.net.URLEncoder

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.model.{HttpResponse, _}
import akka.http.scaladsl.model.headers.BasicHttpCredentials
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.config.{CookieSpecs, RequestConfig}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpUriRequest}
import org.apache.http.impl.client.{BasicCredentialsProvider, CloseableHttpClient, HttpClientBuilder}
import grizzled.slf4j.Logging
import org.slf4j.MDC

import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._


object AkkaBasicAuth extends Logging {

  implicit val system: ActorSystem = ActorSystem("akka-basic-auth")
  implicit val materializer: ActorMaterializer = ActorMaterializer()


  private val ES_USER = "elastic"
  private val ES_PASSWORD = "elastic"

  private val ES_HOST = "192.168.99.100"
  private val ES_PORT = 9206

  def main(args: Array[String]): Unit = {

    executeAsync()

    executeSync()
  }

  def executeAsync(): Unit = {
    logger.info("executeAsync")
    val basePath = "http://" + ES_HOST +":"+ ES_PORT + ""

    val credentials: BasicHttpCredentials = BasicHttpCredentials(ES_USER, ES_PASSWORD)
    val asyncRequest: HttpRequest = RequestBuilding.Get(s"${basePath}/_cluster/health/").addCredentials(credentials)

    val responseFuture: Future[HttpResponse] = Http().singleRequest(asyncRequest)

    val httpResponse = Await.result(responseFuture, 3.second)

    println("response++++")
    println(httpResponse.status)
    println(httpResponse.entity)


    val streamFuture: Future[HttpRequest] = Source.single(asyncRequest).runWith(Sink.head)
    val httpResponse2: HttpMessage = Await.result(streamFuture, 3.second)
    println("response2++++")
    println(httpResponse2)
    println(httpResponse2.entity)
  }

  def executeSync() = {

    val request: HttpRequest = RequestBuilding.Get("/cluster/health/")

    val getRequestAkka: HttpMessage = HttpRequest(
      method = HttpMethods.GET,
      uri = s"/cluster/health/")

  }

  def executeAndClose[T](request: HttpUriRequest)(func: CloseableHttpResponse => T): T = {
    val response = httpClient.execute(request)
    try {
      func(response)
    } finally {
      response.close()
    }
  }

  def buildRequestConfig(): RequestConfig = {
    RequestConfig.custom()
      .setAuthenticationEnabled(true)
      .setCookieSpec(CookieSpecs.DEFAULT)
      .build()
  }

  lazy protected[elasticsearch] val httpClient: CloseableHttpClient = {
    try {
      val connectionUsername: String = ES_USER
      val connectionPassword: String = ES_PASSWORD
      val credsProvider = new BasicCredentialsProvider
      credsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(connectionUsername, connectionPassword))

      val http_config: RequestConfig = buildRequestConfig()

      HttpClientBuilder.create().setDefaultRequestConfig(http_config)
        .setDefaultCredentialsProvider(credsProvider)
        .build()
    } catch {
      case re: RuntimeException =>
        logger.error("Failed to init http-client", re)
        throw re
    }
  }

//  lazy val httpAsyncClient = new ESHttpAsyncClient(EsIndexStorageConfig(async_es_host, async_es_port))

}
