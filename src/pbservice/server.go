package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"
import "errors"

import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type PBServer struct {
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	done       sync.WaitGroup
	finish     chan interface{}
	// Your declarations here.

	view viewservice.View

	// Key => Value
	content     map[string]string
	receive_ids map[int64]string

	mu sync.Mutex
}

func (pb *PBServer) Forward(args *ForwardArgs) error {

	if pb.view.Backup == "" {
		return nil
	}

	var reply ForwardReply
	ok := call(pb.view.Backup, "PBServer.GetForward", args, &reply)
	if !ok {
		return errors.New("forward failed")
	}

	return nil
}

func (pb *PBServer) GetForward(args *ForwardArgs, reply *ForwardReply) error {

	pb.mu.Lock()
	for key, value := range args.Content {
		pb.content[key] = value
	}
	fmt.Println("Get Forward", pb.me, pb.content)
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	pb.mu.Lock()
	fmt.Println("PBServer PUT", args)
	key, value, xid := args.Key, args.Value, args.Xid

	defer pb.mu.Unlock()

	fmt.Println("xid", pb.receive_ids[xid])
	if r, ok := pb.receive_ids[xid]; ok {
		reply.Err = OK
		reply.PreviousValue = r
		return nil
	}

	if args.DoHash {
		reply.PreviousValue = pb.content[key]
		value = strconv.Itoa(int(hash(reply.PreviousValue + value)))
	}

	// forward content to backup
	forwardArgs := &ForwardArgs{map[string]string{key: value}}
	err := pb.Forward(forwardArgs)
	if err != nil {
		return errors.New("forward failed")
	}

	pb.content[key] = value
	pb.receive_ids[xid] = reply.PreviousValue

	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.mu.Lock()
	key := args.Key
	reply.Value = pb.content[key]
	fmt.Println("PBServer GET", key, reply.Value)
	pb.mu.Unlock()
	return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	// Your code here.

	pb.mu.Lock()
	view, err := pb.vs.Ping(pb.view.Viewnum)

	if err != nil {
	}

	fmt.Println("tick", pb.me, pb.view, view)
	// add Backup, view changed
	if pb.me == view.Primary && pb.view.Backup == "" && view.Backup != "" && pb.view.Viewnum != view.Viewnum {
		forwardArgs := &ForwardArgs{Content: pb.content}
		fmt.Println("Forward", forwardArgs)
		pb.view = view
		pb.Forward(forwardArgs)
	}

	pb.view = view
	pb.mu.Unlock()
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	// Your pb.* initializations here.
	pb.content = make(map[string]string)
	pb.receive_ids = make(map[int64]string)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
