package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import de.hpi.ddm.MasterSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Worker extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class WorkerTask implements Serializable {
		private static final long serialVersionUID = 1234567890L; //Wofür ist die eigentlich da?
		private Boolean crackPassword;
		private String[] sha256;
		private char[] charset;
		private char missingChar;
		private String fixedStart;
		private int id;
		private ActorRef master;
		private ActorRef sender;
	}
	
	public static final String DEFAULT_NAME = "worker";

	public static Props props() {
		return Props.create(Worker.class);
	}

	public Worker() {
		this.cluster = Cluster.get(this.context().system());
	}
	
	////////////////////
	// Actor Messages //
	////////////////////

	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
		
		this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(MemberRemoved.class, this::handle)
				.match(Master.TaskMessage.class, this::handle)
				.match(WorkerTask.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private ArrayList<WorkerTask> subdivideTasks(ArrayList<WorkerTask> tasks, int breakLength) {
		ArrayList<WorkerTask> result = new ArrayList<>();

		for (WorkerTask task : tasks) {
			if (task.getFixedStart().length() >= breakLength) {
				this.self().tell(task, this.self());
			} else {
				ArrayList<WorkerTask> subdivide = new ArrayList<>();
				for (int i = 0; i<task.getCharset().length; i++) {
					char[] newCharset = new char[task.getCharset().length-1];
					newCharset = ArrayUtils.remove(task.getCharset(), i);
					String startString = task.getFixedStart() + task.getCharset()[i];
					subdivide.add(new WorkerTask(task.getCrackPassword(), task.getSha256(), newCharset, task.getMissingChar(), startString, task.getId(), task.getMaster(), this.self()));
				}
				result.addAll(this.subdivideTasks(subdivide, breakLength));
			}
		}			
		return result;
	}

	private void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	private void handle(Master.TaskMessage message) {
		ArrayList<WorkerTask> tasks = new ArrayList<>();
		WorkerTask task = new WorkerTask(message.getCrackPassword(), message.getSha256(), message.getCharset(), message.getMissingChar(), "", message.getId(), this.self(), message.getSender());		
		tasks.add(task);
		this.subdivideTasks(tasks, 3); //3 seems to be a good value
		this.log().info("Worker " + this.self().path() + " generated subtasks");
	}

	private void handle(WorkerTask message) {
		ArrayList<String> permutated = new ArrayList<>();
		this.heapPermutation(message.getCharset(), message.getCharset().length, message.getCharset().length, permutated);
		for (String permutate : permutated) {
			String calcHash = this.hash(permutate);
			for (String hash : message.getSha256()) {
				if (calcHash.equals(hash)) {
					this.log().info(permutate+" is a hint for password " + message.getId() + " - Char is: "+message.getMissingChar());
					message.getSender().tell(new Master.ResponseMessage(true, message.getMissingChar(), message.getId(), this.self()), this.self());
					return;
				}
			}
		}
		message.getMaster().tell(new Master.ResponseMessage(false, message.getMissingChar(), message.getId(), this.self()), this.self());
		return;
	}

	private void register(Member member) {
		if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
			this.masterSystem = member;
			
			this.getContext()
				.actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
				.tell(new Master.RegistrationMessage(), this.self());
		}
	}
	
	private void handle(MemberRemoved message) {
		if (this.masterSystem.equals(message.member()))
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
	}
	
	private String hash(String line) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashedBytes = digest.digest(String.valueOf(line).getBytes("UTF-8"));
			
			StringBuffer stringBuffer = new StringBuffer();
			for (int i = 0; i < hashedBytes.length; i++) {
				stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
			}
			return stringBuffer.toString();
		}
		catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	
	private void heapPermutation(char[] a, int size, int n, List<String> l) {
		//What the hell does parameter n do?
		// If size is 1, store the obtained permutation
		if (size == 1)
			l.add(new String(a));

		for (int i = 0; i < size; i++) {
			heapPermutation(a, size - 1, n, l);

			// If size is odd, swap first and last element
			if (size % 2 == 1) {
				char temp = a[0];
				a[0] = a[size - 1];
				a[size - 1] = temp;
			}

			// If size is even, swap i-th and last element
			else {
				char temp = a[i];
				a[i] = a[size - 1];
				a[size - 1] = temp;
			}
		}
	}
}