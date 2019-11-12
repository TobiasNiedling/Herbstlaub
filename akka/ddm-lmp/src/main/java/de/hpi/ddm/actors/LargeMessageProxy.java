package de.hpi.ddm.actors;

import java.io.Serializable;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import akka.stream.*;
import akka.stream.javadsl.*;
import akka.http.javadsl.*;

import de.hpi.ddm.configuration.Configuration;
import de.hpi.ddm.configuration.ConfigurationSingleton;

//TODO Check which are neaded
import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.util.ByteString;

import java.nio.file.Paths;
import java.math.BigInteger;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpEntities;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.HttpMethods.*;
import akka.http.javadsl.model.*;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Directives.*;
import akka.http.javadsl.server.Route;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;

import java.io.IOException;
import java.util.concurrent.CompletionStage;
import akka.japi.function.Function;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	
	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private ActorRef sender;
		private ActorRef receiver;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(BytesMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

		final Configuration c = ConfigurationSingleton.get();

		Materializer materializer = ActorMaterializer.create(this.context().system());

		Source<IncomingConnection, CompletionStage<ServerBinding>> serverSource =
		Http.get(this.context().system()).bind(ConnectHttp.toHost(c.getHost(), 45454), materializer);

		final Function<HttpRequest, HttpResponse> requestHandler =
		new Function<HttpRequest, HttpResponse>() {
		  private final HttpResponse NOT_FOUND =
			HttpResponse.create()
			  .withStatus(404)
			  .withEntity("Unknown resource!");
	  
	  
		  @Override
		  public HttpResponse apply(HttpRequest request) throws Exception {
			Uri uri = request.getUri();
			if (request.method() == HttpMethods.GET) {
			  if (uri.path().equals("/")) {
				return
				  HttpResponse.create()
					.withEntity(ContentTypes.TEXT_HTML_UTF8,
					  "<html><body>Hello world!</body></html>");
			  } else if (uri.path().equals("/hello")) {
				String name = uri.query().get("name").orElse("Mister X");
	  
				return
				  HttpResponse.create()
					.withEntity("Hello " + name + "!");
			  } else if (uri.path().equals("/ping")) {
				return HttpResponse.create().withEntity("PONG!");
			  } else {
				return NOT_FOUND;
			  }
			} else {
			  return NOT_FOUND;
			}
		  }
		};

		CompletionStage<ServerBinding> serverBindingFuture =
		serverSource.to(Sink.foreach(connection -> {
			System.out.println("Accepted new connection from " + connection.remoteAddress());
			connection.handleWith(Flow.of(HttpRequest.class).map(requestHandler), materializer);
			}
		)).run(materializer);

		
  /*final CompletionStage<ServerBinding> binding = 
        http.bindAndHandle(routeFlow, ConnectHttp.toHost("localhost", 8080), 
        materializer);*/

		// This will definitely fail in a distributed setting if the serialized message is large!
		// Solution options:
		// 1. Serialize the object and send its bytes batch-wise (make sure to use artery's side channel then).
		// 2. Serialize the object and send its bytes via Akka streaming.
		// 3. Send the object via Akka's http client-server component.
		// 4. Other ideas ...
		receiverProxy.tell(new BytesMessage<>(message.getMessage(), this.sender(), message.getReceiver()), this.self());
	}

	private void handle(BytesMessage<?> message) {
		// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
		message.getReceiver().tell(message.getBytes(), message.getSender());
	}
}
