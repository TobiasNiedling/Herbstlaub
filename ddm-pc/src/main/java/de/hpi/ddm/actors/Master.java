package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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
    
    private HashMap<Integer,String> hints = new HashMap<>();
    private ArrayList<TaskMessage> taskQueue = new ArrayList<>();
    private HashMap<ActorRef, Integer> actorTasks = new HashMap<>();
    private HashMap<Integer,String> passwordHash = new HashMap<>();
    private char[] globalCharset;
    private Integer hintCount;
	private Integer passwordLength;
	private Integer batchSize;

    //Parameter to determine how many subtasks should be queued (the length of fixed start is maximum this length)
    //2 Worked fine on our home hardware
    //3 works good to not get heart beet errors on low spec hardware
    //4 leads to quite smaller tasks for the workers, but in order to not overtask the master, buffersize should be reduced to ~20
    //Should be as low as possible to efficently use the hardware
    private final int SUBDIVIDE_BREAK_LENGTH = 4; 

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

	//Split the tasks into smaller ones, that can be handled easier
	private ArrayList<TaskMessage> subdivide(ArrayList<TaskMessage> tasks) {

		ArrayList<TaskMessage> result = new ArrayList<>();

		for (TaskMessage task : tasks) {
			if (task.getFixedStart().length() >= Math.min(this.SUBDIVIDE_BREAK_LENGTH, this.passwordLength)) {
				result.add(task);
			} else {
				ArrayList<TaskMessage> subdivide = new ArrayList<>();
				for (int i = 0; i<task.getCharset().length; i++) {
					char[] newCharset = new char[task.getCharset().length-1];
					newCharset = ArrayUtils.remove(task.getCharset(), i);
					String startString = task.getFixedStart() + task.getCharset()[i];
					subdivide.add(new TaskMessage(task.getCrackPassword(), task.getSha256(), newCharset, task.getHint(), startString, task.getId(), i, this.self()));
				}
				result.addAll(this.subdivide(subdivide));
			}
		}			
		return result;
	}

	//Generate tasks to crack hints (remove one character from charset, which could be the hint)
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

	//Assign the generated subtasks to the workers
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

	//If all hints from a batch are cracked, generate Tasks for the passwords
    private void crackPasswords() {
        for (Map.Entry<Integer, String> entry: this.hints.entrySet()) {
            char[] crackCharset = new char[this.globalCharset.length - entry.getValue().length()];
            int j=0;
            for (char hintChar : globalCharset) {
                if (entry.getValue().indexOf(hintChar) > -1) {
                    continue;
                } else {
                    crackCharset[j] = hintChar;
                    j++;
                }
            }
            String[] hashStrings = new String[1];
            hashStrings[0] = this.passwordHash.get(entry.getKey());
            TaskMessage task = new TaskMessage(true, hashStrings, crackCharset, ' ', "", entry.getKey(), this.passwordLength, this.self());
            this.taskQueue.add(task);
            this.schedule();
        }
    }
	
	//Password was cracked by the worker, forward it to the collector and remove the password from the task queue
    protected void handle(CrackedMessage message) {
        if (message.getSuccess() & this.passwordHash.containsKey(message.getPasswordId())) {

			//Remove password from store
            this.hints.remove(message.getPasswordId());
            this.passwordHash.remove(message.getPasswordId());

			//Remove password from queue
            String password = message.getPassword();
            Iterator<TaskMessage> i = this.taskQueue.iterator();
            while(i.hasNext()) {
                TaskMessage task = i.next();
                if (task.getId() == message.getPasswordId()) {
                    i.remove();
                }
			}
			
			//Tell collector
            this.collector.tell(new Collector.CollectMessage(password), this.self());
            if (this.taskQueue.size() == 0) {
				this.collector.tell(new Collector.CollectMessage("Processed batch of size " + batchSize), this.self());
                this.reader.tell(new Reader.ReadMessage(), this.self());
            }
		}
		
		//Worker scheduling
        Integer taskCount = this.actorTasks.getOrDefault(message.getSender(), 1);
        this.actorTasks.put(message.getSender(), taskCount-1);
        this.schedule();
    }

	
	//Hint was cracked (or not), remove unnecessary tasks
    protected void handle(ResponseMessage message) {
        if (message.getSuccess()) {

			//Allready cracked the hint?
			String current = this.hints.getOrDefault(message.getPasswordId(), "");
            if (!(current.indexOf(message.getHint()) > -1)) {
                current += message.getHint();
                this.hints.put(message.getPasswordId(), current);
			}
			
			//Remove tasks
            Boolean finishedPwd = (current.length() == this.hintCount);
			Iterator<TaskMessage> i = this.taskQueue.iterator();
            while(i.hasNext()) {
                TaskMessage task = i.next();
                if (task.getId() == message.getPasswordId() & (finishedPwd | task.getHint() == message.getHint())) {
                    i.remove();
                }
			}

			//Cracked all hints?
            if (this.taskQueue.size() == 0) {
                this.crackPasswords();
            } 
		}
		
		//Worker scheduling
        Integer taskCount = this.actorTasks.getOrDefault(message.getSender(), 1);
        this.actorTasks.put(message.getSender(), taskCount-1);
        this.schedule();
    }
	
	protected void handle(BatchMessage message) {
		
		if (message.getLines().isEmpty()) {
			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.terminate();
			return;
        }
        
        ArrayList<TaskMessage> tasks = new ArrayList<>();
		
		//Extract information from input
		for (String[] line : message.getLines()) {
            
            int id = Integer.parseInt(line[0]);
            
            char[] charset = line[2].toCharArray();
            this.globalCharset = new char[charset.length];
            this.globalCharset = charset;

            int length = Byte.parseByte(line[3]);
            this.passwordLength = length;

            this.passwordHash.put(Integer.parseInt(line[0]), line[4]);
            
            this.hintCount = line.length-5; //Hints start in column 5
			String[] hintInput = new String[this.hintCount]; 
			
			for (int i = 5; i < line.length; i++) { //Hints start in column 5
				hintInput[i-5]=line[i];
			}

			tasks.addAll(this.hintTasks(id, charset, length, hintInput));
		}

		this.batchSize = message.getLines().size();
		
		//Some of the generated subtasks are aiming for the same hint
		//If we would not shuffle the tasks, most of these redundant tasks will run in parallel and then all be executed
		//Due to shuffeling different hints/passwords are cracked in parallel and if a hint is cracked, the chance is higher, 
		//that the redundant task where not allready assigned to a worker
		//So this is only for performance improvement
        Collections.shuffle(tasks);
        this.taskQueue.addAll(tasks);
        this.schedule();
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
