import akka.actor._
import akka.actor.Terminated
import akka.actor.actorRef2Scala
import akka.actor.Actor
import akka.dispatch.Foreach
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.event.LoggingReceive
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import java.io._
import java.security.MessageDigest
import com.typesafe.config.ConfigFactory
import scala.collection.mutable.HashMap
import scala.collection.mutable.Set
import scala.util.control._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.ArrayBuffer


object ChordBonus{

	object Worker{
		case object StartFirst
		case class AddNode(existingNode:ActorRef, addNodeIdentifier:Int, successorActor:ActorRef)
		case class FindSuccessor(startID:Int,addNodeIdentifier:Int, queryType:String, requestActor: ActorRef, successorActor:ActorRef, count:Int)
		case class FindPredecessor(startID:Int, addNodeIdentifier:Int, requestActor: ActorRef, requestResultActor: ActorRef, requestActorReturnType:String, count:Int)
		case object GetPredecessor
		case class UpdateOthers(queryType:String, addNodeIdentifier:Int, predecessorActor:ActorRef)
		case class UpdateFingerTable(currentActor:ActorRef, addNodeIdentifier:Int, nodeFlags:Array[Boolean])
		case class GetSuccessor(startID:Int, existingNode:ActorRef,addNodeIdentifier:Int, requestActor: ActorRef, queryType:String, count:Int)
		case class FindKeyNode(keyArray:Array[Int])
		case class KeyFound(startID:Int, addNodeIdentifier:Int,successorActor:ActorRef,count:Int)
		case object RequestKey
		case class Stablize(addNodeIdentifier:Int, predecessorActor: ActorRef)
		case object Notify
		case class FixFingers(addNodeIdentifier:Int, successorActor:ActorRef)
		case object NodeFailure
		

	}
	class Worker(m:Int,nrOfNodes:Int, nrOfRequests:Int) extends Actor{
		import Worker._
		var master: ActorRef = null
		var fingerTableStart = new Array[Int](m)
		var fingerTableNode = ArrayBuffer.empty[ActorRef]
		var successorList = ArrayBuffer.empty[ActorRef]
		var successorInt = 0
		var successor: ActorRef = null
		var predecessor: ActorRef = null
		var name = context.self.path.name.split(":")
		var nodeNumber = name(2).toInt
		var keyArray = new Array[Int](nrOfRequests)
		var keyCount = 0
		var findFinger = 1
		var currentSuccessor: ActorRef = null
		var currentSuccessorIndex = 0
		
		val loop = new Breaks
		def receive = {
			//-----------------------------------------------------------------------------------------
		    /*
		     * This case stops the node as part of node failure
		     */
		     case NodeFailure => {
		     	context.stop(self)
		     }

			//-----------------------------------------------------------------------------------------
		    /*
		     * This case finds the key node
		     */
		     case FindKeyNode(keyArrayMaster:Array[Int]) => {
		     	keyArray = keyArrayMaster
		     	context.system.scheduler.scheduleOnce(0 milliseconds, self, RequestKey)

		     }

			//-----------------------------------------------------------------------------------------
		    /*
		     * This case is called from FindKeyNode to request key
		     */
		     case RequestKey => {				
		     	if(keyCount < nrOfRequests-1){
		     		keyCount += 1
		     		context.system.scheduler.scheduleOnce(keyCount seconds, self, RequestKey)
		     	}
		     	self ! Worker.FindSuccessor(keyArray(keyCount),keyArray(keyCount),"keyRequest",self, self,0)
		     }

			//-----------------------------------------------------------------------------------------
		    /*
		     * This case prints the key found
		     */			
		     case KeyFound(startID:Int, addNodeIdentifier:Int,successorActor:ActorRef,count:Int) =>{
		     	println("Key: %s found in %s".format(startID, successorActor.path.name))
		     	master ! Master.KeyFound(count)
		     }

			 //-----------------------------------------------------------------------------------------
		    /*
		     * This case is used to create finger table for the first  node.
		     */
		     case StartFirst => {
		     	master = sender
		     	fingerTableNode += self
		     	successor = fingerTableNode(0)
		     	var tempSuccessor = fingerTableNode(0).path.name.split(":")
		     	successorInt = tempSuccessor(2).toInt
		     	for(i<-0 to m-1){
		     		successorList += self
		     		fingerTableStart(i) = (nodeNumber + math.pow(2,i).toInt) % math.pow(2,m).toInt
		     	}
		     	successorList(0) = successor
		     	context.system.scheduler.schedule(0 milliseconds, 5 milliseconds, self, Stablize(-2, self))
		     	context.system.scheduler.schedule(5 milliseconds, 5 milliseconds, self, FixFingers(0, self))
		     	master ! Master.JoinComplete(1)
		     }

			//-----------------------------------------------------------------------------------------
		    /*
		     * This case is used to add a node to existing network.
		     */			
		     case AddNode(existingNode:ActorRef, addNodeIdentifier:Int, successorActor:ActorRef) => {
		     	if(addNodeIdentifier == -1){
		     		master = sender					
		     		for(i<-0 to m-1){
		     			successorList += self
		     			fingerTableStart(i) = (nodeNumber + math.pow(2,i).toInt) % math.pow(2,m).toInt
		     		}
		     		existingNode ! Worker.FindSuccessor(fingerTableStart(0),0,"request",self, existingNode,0)
		     	}
		     	else{
		     		fingerTableNode += successorActor
		     		successor = successorActor
		     		successorList(0) = successor
		     		var tempNodeArrayinti = successorActor.path.name.split(":")
		     		var tempNodeSuccessorinti = tempNodeArrayinti(2).toInt
		     		if(addNodeIdentifier == 0){
		     			successorInt = tempNodeSuccessorinti
		     		}
		     		var temp = self.path.name.split(":")
		     		var selfID = temp(2).toInt					
		     		context.system.scheduler.schedule(0 milliseconds, 5 milliseconds, self, Stablize(-2, self))
		     		context.system.scheduler.schedule(5 milliseconds, 5 milliseconds, self, FixFingers(0, self))
		     		master ! Master.JoinComplete(selfID+1)
		     	}				
		     }	

			//-----------------------------------------------------------------------------------------
		    /*
		     * This case is used to update fingers.
		     */			
		     case FixFingers(addNodeIdentifier:Int, successorActor:ActorRef) => {
		     	if(addNodeIdentifier == 0){
		     		self ! Worker.FindSuccessor(fingerTableStart(findFinger),findFinger,"fingerRequest",self,self,0)
		     		findFinger += 1
		     		findFinger = findFinger % m
		     		if (findFinger == 0){
		     			findFinger = 1
		     		}
		     	}
		     	else{
		     		if(addNodeIdentifier < fingerTableNode.length){
		     			fingerTableNode(addNodeIdentifier) = successorActor
		     		}
		     		else {
		     			fingerTableNode += successorActor
		     		}					
		     	}
		     }

			//-----------------------------------------------------------------------------------------
		    /*
		     * This case runs periodically to stabilize the finger tables of each active node.
		     */						
		     case Stablize(addNodeIdentifier:Int, predecessorActor: ActorRef) => {
		     	context watch successor
		     	if(predecessor != null){
		     		context watch predecessor
		     	}

		     	if(addNodeIdentifier == 0){
		     		successor ! Worker.GetPredecessor
		     	}
		     	else if(addNodeIdentifier == -2){
		     		currentSuccessor = successor
		     		currentSuccessorIndex = 1
		     		var temp = currentSuccessor.path.name.split(":")
		     		var startID = temp(2).toInt
		     		startID += 1
		     		startID = startID % math.pow(2,m).toInt
		     		currentSuccessor ! Worker.FindSuccessor(startID,currentSuccessorIndex,"stablizeRequest",self,currentSuccessor,0)					
		     	}
		     	else if(addNodeIdentifier > 0){
		     		successorList(addNodeIdentifier) = predecessorActor
		     		currentSuccessor = predecessorActor
		     		var temp = currentSuccessor.path.name.split(":")
		     		var startID = temp(2).toInt
		     		startID += 1
		     		startID = startID % math.pow(2,m).toInt
		     		currentSuccessorIndex += 1
		     		if(currentSuccessorIndex < m){
		     			currentSuccessor ! Worker.FindSuccessor(startID,currentSuccessorIndex,"stablizeRequest",self,currentSuccessor,0)
		     		}
		     		else{
		     			self ! Stablize(0, self)
		     		} 
		     	}
		     	else if(addNodeIdentifier == -1){					
		     		var temp = predecessorActor.path.name.split(":")
		     		var startID = temp(2).toInt
		     		if(nodeNumber >= successorInt){
		     			if(startID > nodeNumber && startID <= (math.pow(2,m).toInt)-1){
		     				successor = predecessorActor
		     				successorList(0) = successor
		     				fingerTableNode(0) = predecessorActor
		     				var tempSuccessor = fingerTableNode(0).path.name.split(":")
		     				successorInt = tempSuccessor(2).toInt
		     			}
		     			else if(startID >= 0 && startID < successorInt){
		     				successor = predecessorActor
		     				successorList(0) = successor
		     				fingerTableNode(0) = predecessorActor
		     				var tempSuccessor = fingerTableNode(0).path.name.split(":")
		     				successorInt = tempSuccessor(2).toInt
		     			}
		     		}
		     		else if(startID > nodeNumber && startID < successorInt){
		     			successor = predecessorActor
		     			successorList(0) = successor
		     			fingerTableNode(0) = predecessorActor
		     			var tempSuccessor = fingerTableNode(0).path.name.split(":")
		     			successorInt = tempSuccessor(2).toInt
		     		}
		     		successor ! Worker.Notify
		     	}
		     }

			//-----------------------------------------------------------------------------------------
		    /*
		     * This case is used to notify the successor to make its predecessor as the calling node.
		     */						
		     case Notify => {
		     	var temp = sender.path.name.split(":")
		     	var startID = temp(2).toInt
		     	if(predecessor == null){
		     		predecessor = sender
		     	}
		     	else {
		     		temp = predecessor.path.name.split(":")
		     		var predecessorInt = temp(2).toInt
		     		if(predecessorInt >= nodeNumber){
		     			if(startID > predecessorInt && startID <= (math.pow(2,m).toInt)-1){
		     				predecessor = sender						
		     			}
		     			else if(startID >= 0 && startID < nodeNumber){
		     				predecessor = sender
		     			}
		     		}
		     		else if(startID > predecessorInt && startID < nodeNumber){
		     			predecessor = sender
		     		}
		     	}
		     }

			//-----------------------------------------------------------------------------------------
		    /*
		     * We have used a watcher to monitor when the node fails. If a node fails, the successor list 
		     * of nodes which have that failing node in their finger table are updated
		     */					
		     case Terminated(a) =>{			
		     	if(successor == a){
		     		loop.breakable{
		     			for(j <- 1 to m-1){
		     				if(successorList(j) != a){
		     					successor = successorList(j)
		     					successorList(0) = successor
		     					successorList.remove(j)
		     					successorList += self
		     					fingerTableNode(0) = successor
		     					var tempSuccessor = fingerTableNode(0).path.name.split(":")
		     					successorInt = tempSuccessor(2).toInt
		     					loop.break
		     				}
		     			}
		     		}

		     	}
		     	if(predecessor == a){
		     		predecessor = self
		     	}
		     	context watch successor
		     }

		//-----------------------------------------------------------------------------------------
	    /*
	     * This case returns the predecessor of the calling node.
	     */
	     case GetPredecessor => {
	     	if(predecessor==null){
	     		sender ! Stablize(-1, self)
	     	}
	     	else{
	     		sender ! Stablize(-1, predecessor)
	     	}				
	     }

		//-----------------------------------------------------------------------------------------
	    /*
	     * This case is used to get the successor of the calling node.
	     */			
	     case GetSuccessor(startID:Int, existingNode:ActorRef ,addNodeIdentifier:Int, requestActor: ActorRef, queryType:String, count:Int) => {
	     	var successorActor:ActorRef = fingerTableNode(0)
	     	if(queryType == "reply"){
	     		requestActor ! Worker.AddNode(existingNode, addNodeIdentifier, successorActor)
	     	}
	     	else if(queryType == "keyReply"){
	     		requestActor ! Worker.KeyFound(startID, addNodeIdentifier, successorActor,count)
	     	}
	     	else if(queryType == "fingerReply"){
	     		requestActor ! Worker.FixFingers(addNodeIdentifier, successorActor)
	     	}
	     	else if(queryType == "stablizeReply"){
	     		requestActor ! Worker.Stablize(addNodeIdentifier, successorActor)
	     	}
	     }

		//-----------------------------------------------------------------------------------------
	    /*
	     * This case is used to find the successor of the calling node/
	    */	     
	     case FindSuccessor(startID:Int, addNodeIdentifier:Int, queryType:String, requestActor: ActorRef, successorActor:ActorRef, count:Int) => {
	     	if(queryType == "request"){
	     		self ! Worker.FindPredecessor(startID, addNodeIdentifier, requestActor, self, "successor", 0)
	     	}
	     	else if(queryType == "keyRequest"){
	     		self ! Worker.FindPredecessor(startID, addNodeIdentifier, requestActor, self, "findKey", 0)
	     	}
	     	else if(queryType == "fingerRequest"){
	     		self ! Worker.FindPredecessor(startID, addNodeIdentifier, requestActor, self, "fingerSuccessor", 0)
	     	}
	     	else if(queryType == "stablizeRequest"){
	     		self ! Worker.FindPredecessor(startID, addNodeIdentifier, requestActor, self, "stablizeSuccessor", 0)
	     	}
	     	else if(queryType == "reply"){
	     		successorActor ! Worker.GetSuccessor(startID,self,addNodeIdentifier,requestActor,"reply",count)
	     	}
	     	else if(queryType == "keyReply"){
	     		successorActor ! Worker.GetSuccessor(startID,self,addNodeIdentifier,requestActor,"keyReply",count)
	     	}
	     	else if(queryType == "fingerReply"){
	     		successorActor ! Worker.GetSuccessor(startID,self,addNodeIdentifier,requestActor,"fingerReply",count)
	     	}
	     	else if(queryType == "stablizeReply"){
	     		successorActor ! Worker.GetSuccessor(startID,self,addNodeIdentifier,requestActor,"stablizeReply",count)
	     	}
	     } 

		//-----------------------------------------------------------------------------------------
	    /*
	     * This case is used to find the predecessor of the calling node/
	    */	     
	     case FindPredecessor(startID:Int, addNodeIdentifier:Int, requestActor: ActorRef, requestResultActor: ActorRef, requestActorReturnType:String, count:Int) => {
	     	if(nodeNumber >= successorInt){
	     		if(startID > nodeNumber && startID <= (math.pow(2,m).toInt)-1){
	     			if(requestActorReturnType == "successor"){
	     				requestResultActor ! Worker.FindSuccessor(startID, addNodeIdentifier, "reply", requestActor, self,count)
	     			}
	     			else if(requestActorReturnType == "update"){
	     				requestResultActor ! Worker.UpdateOthers("reply", addNodeIdentifier, self)
	     			}
	     			else if(requestActorReturnType == "findKey"){
	     				requestResultActor ! Worker.FindSuccessor(startID, addNodeIdentifier, "keyReply", requestActor, self,count)
	     			}
	     			else if(requestActorReturnType == "fingerSuccessor"){
	     				requestResultActor ! Worker.FindSuccessor(startID, addNodeIdentifier, "fingerReply", requestActor, self,count)
	     			}
	     			else if(requestActorReturnType == "stablizeSuccessor"){
	     				requestResultActor ! Worker.FindSuccessor(startID, addNodeIdentifier, "stablizeReply", requestActor, self,count)
	     			}
	     		}
	     		else if(startID >= 0 && startID <= successorInt){
	     			if(requestActorReturnType == "successor"){
	     				requestResultActor ! Worker.FindSuccessor(startID, addNodeIdentifier, "reply", requestActor, self,count)
	     			}
	     			else if(requestActorReturnType == "update"){
	     				requestResultActor ! Worker.UpdateOthers("reply", addNodeIdentifier, self)
	     			}
	     			else if(requestActorReturnType == "findKey"){
	     				requestResultActor ! Worker.FindSuccessor(startID, addNodeIdentifier, "keyReply", requestActor, self,count)
	     			}
	     			else if(requestActorReturnType == "fingerSuccessor"){
	     				requestResultActor ! Worker.FindSuccessor(startID, addNodeIdentifier, "fingerReply", requestActor, self,count)
	     			}
	     			else if(requestActorReturnType == "stablizeSuccessor"){
	     				requestResultActor ! Worker.FindSuccessor(startID, addNodeIdentifier, "stablizeReply", requestActor, self,count)
	     			}
	     		}
	     		else {
	     			loop.breakable{
	     				for( i <- fingerTableNode.length-1 to (0, -1)){
	     					var tempNodeArray = fingerTableNode(i).path.name.split(":")
	     					var tempNode = tempNodeArray(2).toInt
	     					if(nodeNumber >= startID){
	     						if(tempNode > nodeNumber && tempNode <= (math.pow(2,m).toInt)-1){
	     							fingerTableNode(i) ! Worker.FindPredecessor(startID, addNodeIdentifier, requestActor, requestResultActor, requestActorReturnType,count+1)
	     							loop.break
	     						}
	     						else if(tempNode >= 0 && tempNode < startID){
	     							fingerTableNode(i) ! Worker.FindPredecessor(startID, addNodeIdentifier, requestActor, requestResultActor, requestActorReturnType,count+1)
	     							loop.break
	     						}
	     					}
	     					else if(tempNode > nodeNumber && tempNode < startID){
	     						fingerTableNode(i) ! Worker.FindPredecessor(startID, addNodeIdentifier, requestActor, requestResultActor, requestActorReturnType,count+1)
	     						loop.break
	     					}
	     				}
	     			}
	     		}
	     	}
	     	else if(startID > nodeNumber && startID <= successorInt){
	     		if(requestActorReturnType == "successor"){
	     			requestResultActor ! Worker.FindSuccessor(startID, addNodeIdentifier, "reply", requestActor, self,count)
	     		}
	     		else if(requestActorReturnType == "update"){
	     			requestResultActor ! Worker.UpdateOthers("reply", addNodeIdentifier, fingerTableNode(0))
	     		}
	     		else if(requestActorReturnType == "findKey"){
	     			requestResultActor ! Worker.FindSuccessor(startID, addNodeIdentifier, "keyReply", requestActor, self,count)
	     		}
	     		else if(requestActorReturnType == "fingerSuccessor"){
	     			requestResultActor ! Worker.FindSuccessor(startID, addNodeIdentifier, "fingerReply", requestActor, self,count)
	     		}
	     		else if(requestActorReturnType == "stablizeSuccessor"){
	     			requestResultActor ! Worker.FindSuccessor(startID, addNodeIdentifier, "stablizeReply", requestActor, self,count)
	     		}
	     	}
	     	else {
	     		loop.breakable{
	     			for( i <- fingerTableNode.length-1 to (0, -1)){
	     				var tempNodeArray = fingerTableNode(i).path.name.split(":")
	     				var tempNode = tempNodeArray(2).toInt
	     				if(nodeNumber >= startID){
	     					if(tempNode > nodeNumber && tempNode <= (math.pow(2,m).toInt)-1){
	     						fingerTableNode(i) ! Worker.FindPredecessor(startID, addNodeIdentifier, requestActor, requestResultActor, requestActorReturnType,count+1)
	     						loop.break
	     					}
	     					else if(tempNode >= 0 && tempNode < startID){
	     						fingerTableNode(i) ! Worker.FindPredecessor(startID, addNodeIdentifier, requestActor, requestResultActor, requestActorReturnType,count+1)										
	     						loop.break
	     					}
	     				}
	     				else if(tempNode > nodeNumber && tempNode < startID){
	     					fingerTableNode(i) ! Worker.FindPredecessor(startID, addNodeIdentifier, requestActor, requestResultActor, requestActorReturnType,count+1)
	     					loop.break
	     				}
	     			}
	     		}
	     	}
	     }

	 }
	} 
	object Master{
		case object Start
		case object Terminate
		case class KeyFound(count:Int)
		case class JoinComplete(addNodeIdentifier:Int)
		case class Shut(i:Int)
		case object NodeFailure
		case object FindKey
	}
	class Master(nrOfNodes: Int, nrOfRequests: Int, m:Int) extends Actor{
		import Master._
		var actorCollection = new HashMap[String, ActorRef]()
		var firstWorker: ActorRef = null	
		var loop = new Breaks
		var counter = 0
		var hopCount:Double = 0
		var isAliveArray = new Array[Boolean](nrOfNodes)
		var isAliveCount = 0
		var precentageFailure:Int = 0
		def receive = {

		//-----------------------------------------------------------------------------------------
	    /*
	     * This case is start point of our program. It is called from the main. This starts the first node.
	    */			
			case Start => {
				//instantiate all actors
				var workerName = "Node:Worker:%s".format(0)
				var worker = context.actorOf(Props(new Worker(m,nrOfNodes,nrOfRequests)), name = workerName)
				actorCollection += (workerName -> worker)
				isAliveArray(0) = true
				println("Worker Created: %s".format(workerName))
				firstWorker = worker
				worker ! Worker.StartFirst
			}

		//-----------------------------------------------------------------------------------------
	    /*
	     * This case is used to check if all the nodes have joined the network./
	    */			
			case JoinComplete(addNodeIdentifier:Int) => {
				loop.breakable{
					for(i <- addNodeIdentifier to nrOfNodes-1){
						var workerName = "Node:Worker:%s".format(i)
						var worker = context.actorOf(Props(new Worker(m,nrOfNodes,nrOfRequests)), name = workerName)
						actorCollection += (workerName -> worker)
						isAliveArray(i) = true
						println("Worker Created: %s".format(workerName))
						worker ! Worker.AddNode(firstWorker, -1, worker)
						loop.break
					}
					if(addNodeIdentifier >= nrOfNodes-1){
						context.system.scheduler.scheduleOnce(2 seconds, self, Master.NodeFailure)
					}
				}
			}

		//-----------------------------------------------------------------------------------------
	    /*
	     * This case is used to search for keys
	    */			
			case Shut(n:Int) => {
				if(n==0){
					println("\nNode Join Complete, The Finger Tables Are As Follows:\n")
				}
				loop.breakable{
					for(i <- n to nrOfNodes-1){
						if(isAliveArray(i) == true){
							var name = "Node:Worker:%s".format(i)
							var tnode = actorCollection.getOrElse(name,null)						
							if(tnode != null){
								loop.break
							}
						}
					}
				}
			}


			case FindKey => {
				if(precentageFailure == 1){
					println("Node 0 failed")
				}
				else{
					println("Nodes Ranging from 0 to %s failed".format(precentageFailure-1))
				}				
				println("Total Number of Nodes Failed = %s (20 precentage of the Whole Network)".format(precentageFailure))
				println("\nSerching For Keys...\n")					
				for(j <- 0 to nrOfNodes-1){
					if(isAliveArray(j) == true) {
						isAliveCount += 1 
						var workerName = "Node:Worker:%s".format(j)
						var worker = actorCollection.getOrElse(workerName,null)
						var keyArray = new Array[Int](nrOfRequests)
						for(k <- 0 to nrOfRequests-1){
							val rnd = new Random
							var key = rnd.nextInt(math.pow(2,m).toInt)
							keyArray(k) = key
						}
						worker ! Worker.FindKeyNode(keyArray)
					}
				}									
			}

		//-----------------------------------------------------------------------------------------
	    /*
	     * This case checks if all the keys have been found. If so, the system is shut down
	    */			
			case KeyFound(count:Int) => {
				counter  += 1
				hopCount += count
				if(counter == nrOfRequests*isAliveCount){
					var totalRequests: Double = nrOfRequests*isAliveCount
					var avgHop: Double = hopCount/totalRequests
					println("\nThe Average Hop Count: %s".format(avgHop))
					println("System Shut Down")
					context.system.shutdown()					
				}
			}

		//-----------------------------------------------------------------------------------------
	    /*
	     * This case fails 20 percent of the total nodes. And then called FindKey to search keys
	     * in the network after it stabilizes
	    */			
			case NodeFailure => {
				println("Node Failure Occurs for 20 % of Total Nodes..\nBelow nodes are terminated")
				precentageFailure = (nrOfNodes * 0.2).toInt
				for(i <- 0 to precentageFailure-1){
					var name = "Node:Worker:%s".format(i)
					var tnode = actorCollection.getOrElse(name,null)
					isAliveArray(i) = false
					println(tnode.path.name)
					context.stop(tnode)
				}
				context.system.scheduler.scheduleOnce(4 seconds, self, Master.FindKey)
			}
		}
	}

	def main (args: Array[String]){
		var nrOfNodes = 0 
		var nrOfRequests = 0
		var m = 31
	    // exit if 2 arguments not passed as command line parameter
	    if (args.length != 2) {
	    	println("Invalid no of args")
	    	System.exit(1)
	    } 
	    else {
	    	nrOfNodes = args(0).toInt
	    	m = math.ceil(math.log(nrOfNodes)/math.log(2)).toInt
	    	if(m < 3){
	    		m = 3
	    	}

	    	nrOfRequests = args(1).toInt
	    	val system = ActorSystem.create("Chord")
	    	val master = system.actorOf(Props(new Master(nrOfNodes, nrOfRequests, m)), name = "Master")
	    	master ! Master.Start
	    }
	}
}