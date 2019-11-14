package de.hpi.ddm.actors;

//General
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.actor.ActorSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

//HTTP
import akka.stream.*;
import akka.stream.javadsl.*;
import akka.http.javadsl.*;
import akka.http.javadsl.model.*;
import akka.http.javadsl.model.HttpMethods.*;
import akka.http.javadsl.server.*;
import akka.http.javadsl.server.Directives.*;

//Configuration
import de.hpi.ddm.configuration.Configuration;
import de.hpi.ddm.configuration.ConfigurationSingleton;

//Serialization
import java.io.Serializable;
import akka.http.javadsl.unmarshalling.Unmarshaller;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.UUID;

//Additional
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import scala.concurrent.ExecutionContextExecutor;
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

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class TransferMessage<T> implements Serializable {
		private T content;
		private String host;
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

	private byte[] getBytesFromMessage(TransferMessage<?> message) {
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(message);
			oos.flush();
			return bos.toByteArray();
		} catch (Exception e) {
			this.log().error("Could not serizalize message");
			return "IOException occurred - C'est la vie.".getBytes();
		}
	}

	private TransferMessage<?> getMessageFromBytes(byte[] data) {
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(data);
			ObjectInputStream ois = new ObjectInputStream(bis);
			TransferMessage<?> result = (TransferMessage<?>) ois.readObject();
			ois.close();
			bis.close();
			return result;
		} catch (Exception e) {
			this.log().error("Could not deserizalize message");
			this.log().error(e.getMessage());
			return new LargeMessageProxy.TransferMessage<>();
		}
	}

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));	
		
		final Configuration c = ConfigurationSingleton.get();

		final byte [] data = getBytesFromMessage(new LargeMessageProxy.TransferMessage<>(message.getMessage(),c.getHost()));
		
		Materializer materializer = ActorMaterializer.create(this.context().system());
		Source<IncomingConnection, CompletionStage<ServerBinding>> serverSource =
		Http.get(this.context().system()).bind(ConnectHttp.toHost(c.getHost(), 45454), materializer);

		UUID uuid = UUID.randomUUID();
		String randomUUIDString = uuid.toString();

		//Return message if /payload/randomID is accessed with GET, else return 404
		final Function<HttpRequest, HttpResponse> requestHandler =
		new Function<HttpRequest, HttpResponse>() {
		  private final HttpResponse NOT_FOUND =
			HttpResponse.create()
			  .withStatus(404);	  
		  @Override
		  public HttpResponse apply(HttpRequest request) throws Exception {
			Uri uri = request.getUri();
			if (request.method() == HttpMethods.GET) {
			  if (uri.path().equals("/payload/"+randomUUIDString)) {
				return
				  HttpResponse.create()
					.withEntity(data);
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
			connection.handleWith(Flow.of(HttpRequest.class).map(requestHandler), materializer);
			}
		)).run(materializer);

		receiverProxy.tell(new BytesMessage<String>("http://"+c.getHost()+":45454/payload/"+randomUUIDString, this.sender(), message.getReceiver()), this.self());
	}

	private void handle(BytesMessage<String> message) {
		String url = message.getBytes();

		Materializer materializer = ActorMaterializer.create(this.context().system());
		final ExecutionContextExecutor dispatcher = this.context().dispatcher();
		final CompletionStage<HttpResponse> response =  
			Http.get(this.context().system()).singleRequest(HttpRequest.create(url), materializer);
		
		response.whenComplete((content, error) -> {
			if (error != null) {
				this.log().error(error.getMessage());
			} else {
				CompletionStage<byte[]> stage = Unmarshaller.entityToByteArray().unmarshal(content.entity(),materializer);
				stage.whenComplete((content2, error2) -> {
					this.log().info(content2.toString());
					TransferMessage<?> deserialized = this.getMessageFromBytes(content2);
					message.getReceiver().tell(deserialized.getContent(), message.getSender());
				});
			}
		});
	}
}
