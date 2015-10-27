import akka.actor._
import scala.math._
import scala.collection._
import java.security.{MessageDigest}
import scala.util._

/**
 * @author mayank
 */


case class lookUp(n:Int)
case class lookUpFinish()
case class makeRequests(numrequest:Int)
case class forwardLookUp(key:Int, node:Int)
case class getSuccessor(node:String)
case class getPredecessor(node:String)
case class updateFingers(node:String)
case class joinNode(nodeId:String)


object Chord_DHT
{
  def main(args: Array[String])
  {
    var numNodes: Int =0;
    var numRequests: Int = 0;
    var numFails: Int =0;
    
    if(args.length<2 || args.length>2)
    {
      numNodes = 256;
      numRequests = 3;  
      println(" *ERROR*: Illegal number of arguments.");
      println(" Taking default values of number of Nodes as "+numNodes+" and number of requests as "+numRequests);         
    }
    else
    {
      numNodes = args(0).toInt;
      numRequests = args(1).toInt;
     // numFails = args(3).toInt;  
    }
    
    //Convert input number of nodes to nearest power of two
/*    var temp:Double =0;
    temp = math.log(numNodes)/math.log(2)
    temp = temp.ceil
    numNodes = (math.pow(2, temp)).toInt;
 */
  
    //Create system for master node and initiate it
    val superSystem = ActorSystem("SuperNodeSys");
    var master = superSystem.actorOf(Props(new SuperNode(numNodes,numRequests)), name="SuperNode")
    
    
  }
}

//#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$
//    Super node definition
//#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$

class SuperNode(nodeCount:Int, reqCount:Int) extends Actor
{
    var numNodes = nodeCount
    var numRequest = reqCount
    var totalHops:Double=0
    var totalLookUps:Double =0
    
    val system = ActorSystem("workerNodes");
    var nodeList = new Array[ActorRef](numNodes)
    var rndm = new Random()
    
    //make n nodes and construct the network topology
    println("Constructing node network ring...")
    for( i <- 0 to numNodes-1)
    {
      nodeList(i) = system.actorOf(Props(new chordNode(i, numNodes)), name="node"+i)
    }
    
    
    //write a snippet that generates a random number and sends that as a request to a node
    
    println("Running 1 request per second on each actor...")
    
    
      for(j <- 0 to numRequest-1)
      {
         for(i <- 0 to numNodes-1)
         {
            var a = rndm.nextInt(numNodes)
            nodeList(i) ! lookUp(a)
            //println("requesting "+a+" to "+i)
          }
         Thread.sleep(1000)
         println(j+1+" Requests per actor processed")
      }
   
    
    /*
     * Receive method in super node has a case for lookup finish, when a node sends back this then
     * super node increments counter for hops and lookup requests completed. When lookup requests 
     * are equal to total requested in beginning then result is computed and printed.
    */
    def receive =
    {
      //case 1 of 3
      case lookUpFinish() =>
      {
          totalHops += 1
          totalLookUps += 1
          //println("finish reported")
          if(totalLookUps == numNodes*numRequest)
          {
            println("Desired number of requests have been completed ")
            println("Each node made "+numRequest+" requests")
            println("Total requests processed "+totalLookUps.toInt+", Total hops traversed "+totalHops.toInt)
            println("Average hop count for each lookup comes out to be "+(totalHops/totalLookUps)) 
            System.exit(1);
          }
      }
      //case 2 of 3
      case forwardLookUp(key,node) =>
      {
        //println("forward key "+key+" to node "+node)
        totalHops += 1
        nodeList(node) ! lookUp(key)    
      }
      //case 3 of 3
      case _ => 
        {
          println("Default case in Super Node")
        }
    }
}

//#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$
//   worker node starts
//#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$


class chordNode(id:Int,nodeCount:Int) extends Actor
{
  var numNodes:Int = nodeCount;
  var nodeId:Int = id;
  var successor: Int = 0;  
  var predecessor:Int = 0;
  
  var hopCount:Int=0
  
  //get hash of nodeId as an array of Byte
  var hashResult:String = sha1Hash(nodeId.toString);
  
  //construct a finger table for this node instance
  val m:Int = (math.log(numNodes)/math.log(2)).toInt;
  var fingerTable =new Array[Int](m);
  
  
  //populate finger table with entries
  for( i <- 0 to m-1)
  {
    fingerTable(i) = ((nodeId+ (math.pow(2, i))) % (numNodes)).toInt;
    //print(nodeId+" => "+fingerTable(i)+", ")
  }

  //Set predecessor and successor of the node
  if(nodeId == 0)
  {
    predecessor = numNodes -1
    successor = 1
  }
  else if(nodeId == numNodes-1)
  {
    predecessor = numNodes-2
    successor = 0
  }
  else
  {
    predecessor = nodeId - 1
    successor = nodeId + 1
  }
  
  
  
  //let it receive requested key from master and return the result of search in finger table to caller
  def receive=
  {
    case lookUp(key) =>
    { 
      //println("searching "+key+" in "+nodeId)
       var first:Int=0
       var second:Int=0
       var i:Int=0
       var flag:Boolean=true
       if(key == nodeId)
        {
         flag=false
          sender ! lookUpFinish()
        }
       else
       {
         while(i<m )
        {  
         
         if(fingerTable(i)==key)
         {
          // println("key "+key+" found in node "+nodeId)
          flag= false
           sender ! lookUpFinish()
         }
         else if(i<m-1)
         {
           if((key< fingerTable(i+1) && key>fingerTable(i)) || fingerTable(i+1)<fingerTable(i))
           {
            // println("requesting firward to "+fingerTable(i)+" for key "+key)
             flag = false
             sender ! forwardLookUp(key,fingerTable(i))
           }
           
         }
         i+=1
        }
       }
       if(flag)
       {
        // println("requesting forward to "+fingerTable(m-1)+" for key "+key)
         sender ! forwardLookUp(key,fingerTable(m-1))
       }
    }
    
    case _ => 
    {
      println("Default case in chordNode")
    }
   
    
  }
  
  def sha1Hash(s: String): String = 
  {
    return MessageDigest.getInstance("SHA-1").digest(s.getBytes).toString
  }
  
  
}

