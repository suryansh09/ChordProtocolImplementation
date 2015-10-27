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


object Chord{

	object Worker{
		case object StartFirst
		case class AddNode(existingNode:ActorRef, addNodeIdentifier:Int, successorActor:ActorRef)
		case class FindSuccessor(startID:Int,addNodeIdentifier:Int, queryType:String, requestActor: ActorRef, successorActor:ActorRef, count:Int)
		case class FindPredecessor(startID:Int, addNodeIdentifier:Int, requestActor: ActorRef, requestResultActor: ActorRef, requestActorReturnType:String, count:Int)
		case class GetPredecessor(existingNode:ActorRef)
		case class UpdateOthers(queryType:String, addNodeIdentifier:Int, predecessorActor:ActorRef)
		case class UpdateFingerTable(currentActor:ActorRef, addNodeIdentifier:Int, nodeFlags:Array[Boolean])
		case class PrintFingerTable(i:Int)
		case class GetSuccessor(startID:Int, existingNode:ActorRef,addNodeIdentifier:Int, requestActor: ActorRef, queryType:String, count:Int)
		case class FindKeyNode(keyArray:Array[Int])
		case class KeyFound(startID:Int, addNodeIdentifier:Int,successorActor:ActorRef,count:Int)
		case object RequestKey
	}
	class Worker(m:Int,nrOfNodes:Int, nrOfRequests:Int) extends Actor{
		import Worker._
		var master: ActorRef = null
		var fingerTableStart = new Array[Int](m)
		var fingerTableNode = new Array[ActorRef](m)
		var successor = 0
		var predecessor: ActorRef = null
		var name = context.self.path.name.split(":")
		var nodeNumber = name(2).toInt
		var keyArray = new Array[Int](nrOfRequests)
		var keyCount = 0
		val loop = new Breaks
		def receive = {
		
		//-----------------------------------------------------------------------------------------
	    /*
	     * This case is used to print the finger table for each node, its predecessor and successor
	     * And tells the master that the node's finger table has been created.
	    */
	     case PrintFingerTable(i:Int) =>{
	     	println("Node Number: %s".format(i))
	     	for(x <- 0 to m-1){
	     		println("%s: %s: %s".format(x, fingerTableStart(x), fingerTableNode(x).path.name))
	     	} 
	     	println("predecessor: %s: successor: %s".format(predecessor.path.name, successor))
	     	master ! Master.Shut(i+1)
	     }

	     //-----------------------------------------------------------------------------------------
	    /*
	     * This case is used to print the finger table for each node, its predecessor and successor
	    */
	     case FindKeyNode(keyArrayMaster:Array[Int]) => {
	     	keyArray = keyArrayMaster
	     	context.system.scheduler.scheduleOnce(0 milliseconds, self, RequestKey)
	     }
	     case RequestKey => {

	     	if(keyCount < nrOfRequests-1){
	     		keyCount += 1
	     		context.system.scheduler.scheduleOnce(keyCount seconds, self, RequestKey)
	     	}
	     	self ! Worker.FindSuccessor(keyArray(keyCount),keyArray(keyCount),"keyRequest",self, self,0)

	     }
	     case KeyFound(startID:Int, addNodeIdentifier:Int,successorActor:ActorRef,count:Int) =>{
	     	println("Key: %s found in: %s ".format(startID, successorActor.path.name))
	     	master ! Master.KeyFound(count)
	     }

	     //-----------------------------------------------------------------------------------------
	    /*
	     * This case is used to create finger table for the first  node.
	    */
     	case StartFirst => {
     		master = sender
			predecessor = self
			for(i<-0 to m-1){
				fingerTableNode(i) = self
				if(i==0){
					var tempSuccessor = fingerTableNode(i).path.name.split(":")
					successor = tempSuccessor(2).toInt						
				}
				fingerTableStart(i) = (nodeNumber + math.pow(2,i).toInt) % math.pow(2,m).toInt
			}
			master ! Master.JoinComplete(1)
			}

		//-----------------------------------------------------------------------------------------
	    /*
	     * This case is used to add a node to existing network.
	    */
		case AddNode(existingNode:ActorRef, addNodeIdentifier:Int, successorActor:ActorRef) => {
			if(addNodeIdentifier == -1){
				master = sender
				predecessor = self
				for(i<-0 to m-1){
					fingerTableStart(i) = (nodeNumber + math.pow(2,i).toInt) % math.pow(2,m).toInt
				}
				existingNode ! Worker.FindSuccessor(fingerTableStart(0),0,"request",self, existingNode,0)
			}
			else if(addNodeIdentifier == -2){
				predecessor = successorActor
				loop.breakable{
					for(i <- 0 to m-2){
						var flag = 0
						var tempStart = fingerTableStart(i+1)
						var tempNodeArray = fingerTableNode(i).path.name.split(":")
						var tempNodeSuccessor = tempNodeArray(2).toInt

						if(nodeNumber >= tempNodeSuccessor){
							if(tempStart >= nodeNumber && tempStart <= (math.pow(2,m).toInt)-1){
								flag = 1
								fingerTableNode(i+1) = fingerTableNode(i)
							}
							else if(tempStart >= 0 && tempStart < tempNodeSuccessor){
								flag = 1
								fingerTableNode(i+1) = fingerTableNode(i)
							}
						}
						if(tempStart >= nodeNumber && tempStart < tempNodeSuccessor){
							flag = 1
							fingerTableNode(i+1) = fingerTableNode(i)}
						if(flag == 0){
							existingNode ! Worker.FindSuccessor(fingerTableStart(i+1),i+1,"request",self, existingNode,0)
							loop.break
						}
					}
					self ! Worker.UpdateOthers("request", 0,self)					
				}
			}
			else{
				fingerTableNode(addNodeIdentifier) = successorActor
				var tempNodeArrayinti = successorActor.path.name.split(":")
				var tempNodeSuccessorinti = tempNodeArrayinti(2).toInt
				if(addNodeIdentifier == 0){
					successor = tempNodeSuccessorinti
				}
				if(addNodeIdentifier == 0){
					var tempSuccessor = fingerTableNode(0).path.name.split(":")
					successor = tempSuccessor(2).toInt
					successorActor ! Worker.GetPredecessor(existingNode)
				}
				else{
					var i = addNodeIdentifier
					loop.breakable{
						for(i <- i to m-2){
							var flag = 0
							var tempStart = fingerTableStart(i+1)
							var tempNodeArray = fingerTableNode(i).path.name.split(":")
							var tempNodeSuccessor = tempNodeArray(2).toInt
							if(nodeNumber >= tempNodeSuccessor){
								if(tempStart >= nodeNumber && tempStart <= (math.pow(2,m).toInt)-1){
									flag = 1
									fingerTableNode(i+1) = fingerTableNode(i)
								}
								else if(tempStart >= 0 && tempStart < tempNodeSuccessor){
									flag = 1
									fingerTableNode(i+1) = fingerTableNode(i)
								}
							}
							if(tempStart >= nodeNumber && tempStart < tempNodeSuccessor){
								flag = 1
								fingerTableNode(i+1) = fingerTableNode(i)
							}
							if(flag == 0){
								existingNode ! Worker.FindSuccessor(fingerTableStart(i+1),i+1,"request",self, existingNode,0)
								loop.break
							}
						}
						self ! Worker.UpdateOthers("request", 0, self)	
					}
				}
			}
		}

		//-----------------------------------------------------------------------------------------
	    /*
	     * This case is used to update the finger tables of other nodes.
	    */
		case UpdateOthers(queryType:String, addNodeIdentifier:Int, predecessorActor:ActorRef) => {
			var startID = 0
			if(queryType == "request"){
				startID = nodeNumber - math.pow(2,addNodeIdentifier).toInt
				if(startID < 0){
					startID = math.pow(2, m).toInt + startID
				}
				self ! FindPredecessor(startID, addNodeIdentifier, self, self, "update", 0)				
			}
			else if(queryType == "reply"){
				var tempNodeArray = predecessorActor.path.name.split(":")
				var num = tempNodeArray(2).toInt
				var nodeFlags = new Array[Boolean](nrOfNodes)
				predecessorActor ! Worker.UpdateFingerTable(self,addNodeIdentifier,nodeFlags)
			}
			else if(queryType == "returnControl"){
				if(addNodeIdentifier < m){
					self ! Worker.UpdateOthers("request", addNodeIdentifier, self)
				}
				else{
					master ! Master.JoinComplete(nodeNumber+1)
				}
			}
		}

		//---------------------------------------------------------------------------------------
	    /*
	     * This case is called by UpdateOthers and is used to update the finger table node.
	    */
		case UpdateFingerTable(currentActor:ActorRef, addNodeIdentifier:Int, nodeFlags:Array[Boolean]) => {
			var p:ActorRef = null
			var tempNodeArray = currentActor.path.name.split(":")
			var s = tempNodeArray(2).toInt
			tempNodeArray = self.path.name.split(":")
			var n = tempNodeArray(2).toInt
			tempNodeArray = fingerTableNode(addNodeIdentifier).path.name.split(":")
			var nFingerNode = tempNodeArray(2).toInt
			if(s>=fingerTableStart(addNodeIdentifier) && s != n){
				var flag = 0
				if(n >= nFingerNode){
					if(s >= n && s <= (math.pow(2,m).toInt)-1){
						flag = 1
						fingerTableNode(addNodeIdentifier) = currentActor
						if(addNodeIdentifier == 0){
							successor = s
						}
						p = predecessor
						p ! Worker.UpdateFingerTable(currentActor,addNodeIdentifier,nodeFlags)
						nodeFlags(n) = true
					}
					else if(s >= 0 && s < nFingerNode){
						flag = 1
						fingerTableNode(addNodeIdentifier) = currentActor
						if(addNodeIdentifier == 0){
							successor = s
						}
						p = predecessor
						p ! Worker.UpdateFingerTable(currentActor,addNodeIdentifier,nodeFlags)
						nodeFlags(n) = true
					}
				}
				if(s >= n && s < nFingerNode){
					flag = 1
					fingerTableNode(addNodeIdentifier) = currentActor
					if(addNodeIdentifier == 0){
						successor = s
					}
					p = predecessor
					p ! Worker.UpdateFingerTable(currentActor,addNodeIdentifier,nodeFlags)
					nodeFlags(n) = true
				}
				if(flag == 0){
					currentActor ! Worker.UpdateOthers("returnControl", addNodeIdentifier+1,currentActor)
					nodeFlags(n) = true
				}
			}
			else if(nodeFlags(n) == false){
				nodeFlags(n) = true
				p = predecessor
				p ! Worker.UpdateFingerTable(currentActor,addNodeIdentifier,nodeFlags)
			}
			else{
				nodeFlags(n) = true
				currentActor ! Worker.UpdateOthers("returnControl", addNodeIdentifier+1,currentActor)
			}
		}

		//-----------------------------------------------------------------------------------------
	    /*
	     * This case returns the predecessor of the calling node.
	    */
		case GetPredecessor(existingNode:ActorRef) => {
			var tempPredecessor = predecessor
			predecessor = sender
			sender ! AddNode(existingNode, -2, tempPredecessor)
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
			else if(queryType == "reply"){
				successorActor ! Worker.GetSuccessor(startID,self,addNodeIdentifier,requestActor,"reply",count)
			}
			else if(queryType == "keyReply"){
				successorActor ! Worker.GetSuccessor(startID,self,addNodeIdentifier,requestActor,"keyReply",count)
			}
		} 

		//-----------------------------------------------------------------------------------------
	    /*
	     * This case is used to find the predecessor of the calling node/
	    */
		case FindPredecessor(startID:Int, addNodeIdentifier:Int, requestActor: ActorRef, requestResultActor: ActorRef, requestActorReturnType:String, count:Int) => {			
			if(nodeNumber >= successor){
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
				}
				else if(startID >= 0 && startID <= successor){
					if(requestActorReturnType == "successor"){
						requestResultActor ! Worker.FindSuccessor(startID, addNodeIdentifier, "reply", requestActor, self,count)
					}
					else if(requestActorReturnType == "update"){
						requestResultActor ! Worker.UpdateOthers("reply", addNodeIdentifier, self)
					}
					else if(requestActorReturnType == "findKey"){
						requestResultActor ! Worker.FindSuccessor(startID, addNodeIdentifier, "keyReply", requestActor, self,count)
					}
				}
				else {
					loop.breakable{
						for( i <- m-1 to (0, -1)){
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
			else if(startID > nodeNumber && startID <= successor){
				if(requestActorReturnType == "successor"){
					requestResultActor ! Worker.FindSuccessor(startID, addNodeIdentifier, "reply", requestActor, self,count)
				}
				else if(requestActorReturnType == "update"){
					requestResultActor ! Worker.UpdateOthers("reply", addNodeIdentifier, fingerTableNode(0))
				}
				else if(requestActorReturnType == "findKey"){
						requestResultActor ! Worker.FindSuccessor(startID, addNodeIdentifier, "keyReply", requestActor, self,count)
					}
				}
				else {
				loop.breakable{
					for( i <- m-1 to (0, -1)){
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
}
class Master(nrOfNodes: Int, nrOfRequests: Int, m:Int) extends Actor{
	import Master._
	var actorCollection = new HashMap[String, ActorRef]()
	var firstWorker: ActorRef = null	
	var loop = new Breaks
	var counter = 0
	var hopCount:Double = 0
	def receive = {

		//-----------------------------------------------------------------------------------------
	    /*
	     * This case is start point of our program. It is called from the main. This starts the first node.
	    */
		case Start => {
			var workerName = "Node:Worker:%s".format(0)
			var worker = context.actorOf(Props(new Worker(m,nrOfNodes,nrOfRequests)), name = workerName)
			actorCollection += (workerName -> worker)
			println("Worker Created: %s".format(workerName))
			firstWorker = worker
			worker ! Worker.StartFirst
		}

		//-----------------------------------------------------------------------------------------
	    /*
	     * This case is used to check if all the nodes have joined the network./
	    */
		case JoinComplete(addNodeIdentifier:Int) => {
			//checks if all the nodes have joined the network
			loop.breakable{
				for(i <- addNodeIdentifier to nrOfNodes-1){
					var workerName = "Node:Worker:%s".format(i)
					var worker = context.actorOf(Props(new Worker(m,nrOfNodes,nrOfRequests)), name = workerName)
					actorCollection += (workerName -> worker)
					println("Worker Created: %s".format(workerName))

					worker ! Worker.AddNode(firstWorker, -1, worker)
					loop.break
				}
				if(addNodeIdentifier >= nrOfNodes-1){
					self ! Master.Shut(0)
				}
			}
		}

		//-----------------------------------------------------------------------------------------
	    /*
	     * This case calls the PrintFingerTable function and is used to search for keys/
	    */
		case Shut(n:Int) => {
			if(n==0){
				println("\nNode Join Complete, The Finger Tables Are As Follows:")
			}
			loop.breakable{
				for(i <- n to nrOfNodes-1){
					var name = "Node:Worker:%s".format(i)
					var tnode = actorCollection.getOrElse(name,null)
					tnode ! Worker.PrintFingerTable(i)
					loop.break
				}				
			}
			if(n > nrOfNodes-1) {
				println("\nSerching For Keys...\n")
				for(j <- 0 to nrOfNodes-1){
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
			if(counter == nrOfRequests*nrOfNodes){
				var totalRequests: Double = nrOfRequests*nrOfNodes
				var avgHop: Double = hopCount/totalRequests
				println("\nThe Average Hop Count: %s".format(avgHop))
				println("System Shut Down")
				context.system.shutdown()					
			}
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