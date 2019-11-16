package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
		private String fixedStart;
		private int id;
		private int subtaskId;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class ResponseMessage implements Serializable {
		private static final long serialVersionUID = 987654321L; //Wofür ist die eigentlich da?
		private Boolean success;
		private String cracked;
		private int id;
		private int subtaskId;
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
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();
		
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	private ArrayList<TaskMessage> subdivideTasks(ArrayList<TaskMessage> tasks, int breakLength) {
		ArrayList<TaskMessage> result = new ArrayList<>();

		for (TaskMessage task : tasks) {
			if (task.getFixedStart().length() >= breakLength) {
				result.add(task);
			} else {
				ArrayList<TaskMessage> subdivide = new ArrayList<>();
				for (int i = 0; i<task.getCharset().length; i++) {
					char[] newCharset = new char[task.getCharset().length-1];
					newCharset = ArrayUtils.remove(task.getCharset(), i);
					String startString = task.getFixedStart() + task.getCharset()[i];
					subdivide.add(new TaskMessage(task.getCrackPassword(), task.getSha256(), newCharset, task.getMissingChar(), startString, task.getId(), 0));
				}
				result.addAll(this.subdivideTasks(subdivide, breakLength));
			}
		}			
		return result;
	}

	private ArrayList<TaskMessage> hintTasks(int id, char[] charset, byte length, String password, String[] hints) {

		ArrayList<TaskMessage> result = new ArrayList<>();
		for (int j = 0; j < charset.length; j++) {
			char[] reducedCharset = new char[charset.length-1];
			reducedCharset = ArrayUtils.remove(charset, j);
			result.add(new TaskMessage(false,hints,reducedCharset,charset[j],"",id, 0));
		}

		//Find a break length, that the master is not stucked ("3"), but the workers stillt have something to do ("8")
		int breakLength = Math.min(3,result.get(0).getCharset().length-8);

		return this.subdivideTasks(result, breakLength);
	}
	
	protected void handle(BatchMessage message) {
		
		///////////////////////////////////////////////////////////////////////////////////////////////////////
		// The input file is read in batches for two reasons: /////////////////////////////////////////////////
		// 1. If we distribute the batches early, we might not need to hold the entire input data in memory. //
		// 2. If we process the batches early, we can achieve latency hiding. /////////////////////////////////
		// TODO: Implement the processing of the data for the concrete assignment. ////////////////////////////
		///////////////////////////////////////////////////////////////////////////////////////////////////////
		
		if (message.getLines().isEmpty()) {
			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.terminate();
			return;
		}
		
		for (String[] line : message.getLines()) {
			//TODO Remove test limitation
			if (!line[0].equals("1")) {
				//continue;
			}
			this.log().info("Now processing password ID " + line[0].toString() + " - " + line[1].toString());
			int id = Integer.parseInt(line[0]);
			char[] charset = line[2].toCharArray();
			byte length = Byte.parseByte(line[3]);
			String password = line[4];
			String[] hints = new String[line.length-5];
			
			for (int i = 5; i < line.length; i++) {
				hints[i-5]=line[i];
			}

			this.log().info("\tCharset is " + Arrays.toString(charset) + ", lenght is " + length + ", number of hints is " + hints.length);

			ArrayList<TaskMessage> tasks = this.hintTasks(id, charset, length, password, hints);

			int tasksSize = tasks.size();
			this.log().info("Generated " + tasksSize + " subtasks");

			for (int i=0; i < tasksSize; i++) {
				int workerNum = i % this.workers.size();
				tasks.get(i).setSubtaskId(i);
				this.workers.get(workerNum).tell(tasks.get(i), this.self());
			}
		}
		
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
