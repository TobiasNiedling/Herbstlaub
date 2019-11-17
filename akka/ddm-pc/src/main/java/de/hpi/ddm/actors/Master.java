package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.esotericsoftware.reflectasm.shaded.org.objectweb.asm.ByteVector;

import org.apache.commons.lang3.ArrayUtils;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.stream.stage.TimerMessages.Scheduled;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
    public static final String DEFAULT_NAME = "master";
    
    private HashMap<Integer,String> hints = new HashMap<>();
    private ArrayList<TaskMessage> taskQueue = new ArrayList<>();
    private HashMap<ActorRef, Integer> actorTasks = new HashMap<>();
    private HashMap<Integer,String> passwordHash = new HashMap<>();
    private char[] globalCharset;
    private Integer hintCount;
    private Integer passwordLength;

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
		private char hint;
		private String fixedStart;
		private int id;
        private int subtaskId;
        private ActorRef sender;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class ResponseMessage implements Serializable {
		private static final long serialVersionUID = 987654321L; //Wofür ist die eigentlich da?
		private char hint;
        private int passwordId;
        private Boolean success;
        private ActorRef sender;
    }
    
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class CrackedMessage implements Serializable {
		private static final long serialVersionUID = 987654321L; //Wofür ist die eigentlich da?
		private String password;
        private int passwordId;
        private Boolean success;
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
                .match(CrackedMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();
		
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	private ArrayList<TaskMessage> subdivide(ArrayList<TaskMessage> tasks) {

		ArrayList<TaskMessage> result = new ArrayList<>();

		for (TaskMessage task : tasks) {
			if (task.getFixedStart().length() >= 3) {
				result.add(task);
			} else {
				ArrayList<TaskMessage> subdivide = new ArrayList<>();
				for (int i = 0; i<task.getCharset().length; i++) {
					char[] newCharset = new char[task.getCharset().length-1];
					newCharset = ArrayUtils.remove(task.getCharset(), i);
					String startString = task.getFixedStart() + task.getCharset()[i];
					subdivide.add(new TaskMessage(task.getCrackPassword(), task.getSha256(), newCharset, task.getHint(), startString, task.getId(), 0, this.self()));
				}
				result.addAll(this.subdivide(subdivide));
			}
		}			
		return result;
	}

	private ArrayList<TaskMessage> hintTasks(int id, char[] charset, int length, String[] hintInput) {

		ArrayList<TaskMessage> result = new ArrayList<>();
		for (int i = 0; i < charset.length; i++) {
			char[] reducedCharset = new char[charset.length-1];
			reducedCharset = ArrayUtils.remove(charset, i);
			result.add(new TaskMessage(false,hintInput,reducedCharset,charset[i],"",id, 0, this.self()));
		}

		return this.subdivide(result);
        //return result;
    }

    private void schedule() {
        if (this.taskQueue.size() > 0) {
            for (ActorRef actor: this.workers) {
                Integer currentTasks = this.actorTasks.getOrDefault(actor, 0);
                while (currentTasks < 3) {
                    TaskMessage task = this.taskQueue.remove(0);
                    actor.tell(task, this.self());
                    this.taskQueue.add(task); // Append task again in case worker fails
                    currentTasks += 1;
                    this.actorTasks.put(actor, currentTasks);
                }
            }
        }
    }

    private void crackPasswords() {
        for (int i = 1; i <= this.hints.size(); i++) {

            char[] crackCharset = new char[this.globalCharset.length - this.hints.get(i).length()];
            int j=0;
            for (char hintChar : globalCharset) {
                if (this.hints.get(i).indexOf(hintChar) > -1) {
                    continue;
                } else {
                    crackCharset[j] = hintChar;
                    j++;
                }
            }
            String[] hashStrings = new String[1];
            hashStrings[0] = this.passwordHash.get(i);
            TaskMessage task = new TaskMessage(true, hashStrings, crackCharset, ' ', "", i, this.passwordLength, this.self());
            this.taskQueue.add(task);
            this.schedule();
        }
    }
    
    protected void handle(CrackedMessage message) {
        if (message.getSuccess()) {
            String password = message.getPassword();
            int passwordId = message.getPasswordId();
            this.log().info("Cracked password number " + passwordId + ", it is: " + password);
            ArrayList<TaskMessage> newQueue = new ArrayList<>();
            Iterator<TaskMessage> i = this.taskQueue.iterator();
            while(i.hasNext()) {
                TaskMessage task = i.next();
                if (task.getId() != message.getPasswordId()) {
                    newQueue.add(task);
                }
            }
            this.taskQueue = newQueue;
            this.collector.tell(new Collector.CollectMessage(password), this.self());
        }
    }

    protected void handle(ResponseMessage message) {
        if (message.getSuccess()) {
            String current = this.hints.getOrDefault(message.getPasswordId(), "");
            if (!(current.indexOf(message.getHint()) > -1)) {
                current += message.getHint();
                this.hints.put(message.getPasswordId(), current);
            }
            Boolean finishedPwd = (current.length() == this.hintCount);
            ArrayList<TaskMessage> newQueue = new ArrayList<>();
            Iterator<TaskMessage> i = this.taskQueue.iterator();
            while(i.hasNext()) {
                TaskMessage task = i.next();
                if (task.getId() != message.getPasswordId() | (!finishedPwd & task.getHint() != message.getHint())) {
                    newQueue.add(task);
                }
            }
            this.taskQueue = newQueue;
            this.log().info("Task queue length: "+this.taskQueue.size());  
            if (this.taskQueue.size() == 0) {
                this.crackPasswords();
            } 
        }
        Integer taskCount = this.actorTasks.getOrDefault(message.getSender(), -1);
        this.actorTasks.put(message.getSender(), taskCount-1);
        schedule();
    }
	
	protected void handle(BatchMessage message) {
		
		if (message.getLines().isEmpty()) {
			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.terminate();
			return;
        }
        
        ArrayList<TaskMessage> tasks = new ArrayList<>();
		
		for (String[] line : message.getLines()) {
			//TODO Remove test limitation
			if (!line[0].equals("1")) {
				continue;
			}
			this.log().info("Now processing password ID " + line[0].toString() + " - " + line[1].toString());
			int id = Integer.parseInt(line[0]);
            char[] charset = line[2].toCharArray();
            this.globalCharset = new char[charset.length];
            this.globalCharset = charset;
            int length = Byte.parseByte(line[3]);
            this.passwordLength = length;
            this.hintCount = line.length-5; //Hints start in column 5
			this.passwordHash.put(Integer.parseInt(line[0]), line[4]);
			String[] hintInput = new String[this.hintCount]; 
			
			for (int i = 5; i < line.length; i++) { //Hints start in column 5
				hintInput[i-5]=line[i];
			}

			tasks.addAll(this.hintTasks(id, charset, length, hintInput));

			int tasksSize = tasks.size();
            this.log().info("Generated " + tasksSize + " subtasks");
        }

        Collections.shuffle(tasks);
        this.taskQueue.addAll(tasks);
        this.schedule();
        
		this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}
	
	protected void terminate() {
		/*this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		for (ActorRef worker : this.workers) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
		
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);*/
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
