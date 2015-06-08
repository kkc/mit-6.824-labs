package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	curr         View
	primaryAcked bool
	//lastSeen     map[string]time.Time
	pingTick    uint
	currentTick uint
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	vs.mu.Lock()
	defer vs.mu.Unlock()
	// Your code here.
	id, viewnum := args.Me, args.Viewnum

	if vs.curr.Primary == "" {
		vs.curr.Viewnum = 1
		vs.curr.Primary = id
		vs.pingTick = vs.currentTick
	} else if id == vs.curr.Primary {
		if viewnum == vs.curr.Viewnum {
			vs.primaryAcked = true
		} else if viewnum == 0 {
			vs.curr.Primary = vs.curr.Backup
			vs.curr.Backup = ""
			vs.curr.Viewnum += 1
			vs.primaryAcked = false
		}
		vs.pingTick = vs.currentTick
	} else if vs.curr.Backup == "" && vs.primaryAcked == true {
		vs.curr.Viewnum += 1
		vs.curr.Backup = id
		vs.primaryAcked = false
	}

	//fmt.Println(vs.curr)
	reply.View = vs.curr
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	vs.mu.Lock()
	defer vs.mu.Unlock()
	// Your code here.
	reply.View = vs.curr

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	vs.mu.Lock()
	defer vs.mu.Unlock()
	// Your code here.
	vs.currentTick += 1

	if vs.currentTick-vs.pingTick >= DeadPings {
		if vs.primaryAcked {
			vs.curr.Primary = vs.curr.Backup
			vs.curr.Backup = ""
			vs.curr.Viewnum += 1
			vs.primaryAcked = false
			//fmt.Println(vs.curr)
		}
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
