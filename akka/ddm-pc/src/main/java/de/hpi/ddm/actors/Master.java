package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "master";

	private ArrayList<TaskMessage> taskQueue = new ArrayList<>();
	private HashMap<Integer,String> hintMap = new HashMap<>();
	private HashMap<ActorRef,Boolean> actorsBusyMap = new HashMap<>();
	private int hintCount;

	public static Props props(final ActorRef reader, final ActorRef collector) {
		return Props.create(Master.class, () -> new Master(reader, collector));
	}

	public Master(final ActorRef reader, final ActorRef collector) {
		this.reader = reader;
		this.collector = collector;
		this.workers = new ArrayList<>();
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	public static class StartMessage implements Serializable {
		private static final long serialVersionUID = -50374816448627600L;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BatchMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private List<String[]> lines;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class TaskMessage implements Serializable {
		private static final long serialVersionUID = 123456789L; //Wofür ist die eigentlich da?
		private Boolean crackPassword;
		private String[] sha256;
		private char[] charset;
		private char missingChar;
		private int id;
		private ActorRef sender;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class ResponseMessage implements Serializable {
		private static final long serialVersionUID = 987654321L; //Wofür ist die eigentlich da?
		private Boolean success;
		private char hint;
		private int passwordId;
		private ActorRef sender;
	}

	@Data
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;

	private long startTime;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(StartMessage.class, this::handle)
				.match(BatchMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(RegistrationMessage.class, this::handle)
				.match(ResponseMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();
		
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	private ArrayList<TaskMessage> hintTasks(int id, char[] charset, byte length, String password, String[] hints) {

		ArrayList<TaskMessage> result = new ArrayList<>();
		for (int j = 0; j < charset.length; j++) {
			char[] reducedCharset = new char[charset.length-1];
			reducedCharset = ArrayUtils.remove(charset, j);
			result.add(new TaskMessage(false,hints,reducedCharset,charset[j],id, this.self()));
		}

		return result;
	}

	private void schedule() {
		this.log().info("Scheduling...");
		for (ActorRef actor : this.workers) {
			this.log().info("Checking actor " + actor.path());
			if (this.actorsBusyMap.getOrDefault(actor, false)) {
				continue;
			}	
			this.log().info("Scheduling to actor " + actor.path());		
			TaskMessage nextTask = this.taskQueue.remove(0);
			this.taskQueue.add(nextTask); //Move nextTask to the end of queue, if worker fails
			this.actorsBusyMap.put(actor, true);
			actor.tell(nextTask, this.self());
		}
		this.log().info("Finished scheduling");
	}

	protected void handle(ResponseMessage message) {
		this.log().info("Got response from actor " + message.getSender().path());
		if (message.getSuccess()) {
			String current = hintMap.getOrDefault(message.getPasswordId(), "");
			if (!(current.indexOf(message.getHint()) > -1)) current += message.getHint();
			hintMap.put(message.getPasswordId(), current);
			Boolean finished = (current.length() == this.hintCount);
			for(TaskMessage task: this.taskQueue) {
				if (task.getId() == message.getPasswordId() & (task.getMissingChar() == message.getHint() | finished)) {
					this.taskQueue.remove(task);
				}
			}
		} else {
			for(TaskMessage task: this.taskQueue) {
				if (task.getId() == message.getPasswordId() & task.getMissingChar() == message.getHint()) {
					this.taskQueue.remove(task);
				}
			}		
		}
		this.actorsBusyMap.put(message.getSender(), false);
		Integer size = this.actorsBusyMap.size();
		this.log().info(size.toString());
		this.schedule();
	}
	
	protected void handle(BatchMessage message) {
		
		if (message.getLines().isEmpty()) {
			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.terminate();
			return;
		}
		
		for (String[] line : message.getLines()) {

			int id = Integer.parseInt(line[0]);
			char[] charset = line[2].toCharArray();
			byte length = Byte.parseByte(line[3]);
			String password = line[4];
			this.hintCount = line.length-5;
			String[] hints = new String[line.length-5]; //Hints start in column 5
			
			for (int i = 5; i < line.length; i++) { //Hints start in column 5
				hints[i-5]=line[i];
			}

			ArrayList<TaskMessage> tasks = this.hintTasks(id, charset, length, password, hints);			
			this.taskQueue.addAll(tasks);
		}
		this.log().info("Initial scheduling");	
		this.schedule();		
		this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}
	
	protected void terminate() {
		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		for (ActorRef worker : this.workers) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
		
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.workers.add(this.sender());
//		this.log().info("Registered {}", this.sender());
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
//		this.log().info("Unregistered {}", message.getActor());
	}
}
