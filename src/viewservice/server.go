package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	lastPingTime map[string]time.Time
	view         map[string]View
	acknowledged bool
	idleServer   []string
}

func stringInSlice(server string, idleServer []string) bool {
	for _, v := range idleServer {
		if server == v {
			return true
		}
	}
	return false
}

func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.lastPingTime[args.Me] = time.Now()

	//viewservice first starts
	if len(vs.view) == 0 {
		currentView := View{}
		currentView.Primary = args.Me
		currentView.Viewnum += 1
		vs.view["current"] = currentView
		vs.acknowledged = false

		reply.View = vs.view["current"]
		return nil
	}

	currentView, ok := vs.view["current"]

	if ok == true {
		nextView, ok := vs.view["next"]
		if ok == false {
			nextView = View{}
			nextView.Primary = currentView.Primary
		}

		if nextView.Primary == "" {
			nextView.Primary = args.Me
			vs.view["next"] = nextView
			if vs.acknowledged == true {
				vs.view["current"] = nextView
				vs.acknowledged = false
			}
		} else {
			// if current view has not been acked, viewservice will proceed to next view.
			if args.Me != currentView.Primary {
				if nextView.Backup == "" {
					nextView.Backup = args.Me
					nextView.Viewnum = currentView.Viewnum + 1
					vs.view["next"] = nextView
					if vs.acknowledged == true {
						vs.view["current"] = nextView
						vs.acknowledged = false
					}
				}

				if currentView.Backup != "" && args.Me != currentView.Backup {
					if stringInSlice(args.Me, vs.idleServer) == false {
						vs.idleServer = append(vs.idleServer, args.Me)
					}
				}
			} else {
				//acknowledges current view
				if args.Viewnum == currentView.Viewnum {
					vs.acknowledged = true
				} else if args.Viewnum == 0 {
					nextView.Primary = nextView.Backup
					nextView.Backup = args.Me
					nextView.Viewnum = vs.view["current"].Viewnum + 1
					vs.view["next"] = nextView
					if vs.acknowledged == true {
						vs.view["current"] = vs.view["next"]
						vs.acknowledged = false
					}
				}
			}
		}
	}

	reply.View = vs.view["current"]
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View = vs.view["current"]

	return nil
}

func (vs *ViewServer) expired(serverName string) bool {
	lastPingTime := vs.lastPingTime[serverName]
	elapsedTime := time.Now().Sub(lastPingTime)
	if elapsedTime > DeadPings*PingInterval {
		return true
	}
	return false
}

func (vs *ViewServer) getIdleServer() string {
	var idleServer string
	for len(vs.idleServer) > 0 {
		idleServer, vs.idleServer = vs.idleServer[0], vs.idleServer[1:]
		if vs.expired(idleServer) == true {
			continue
		}
		break
	}
	return idleServer
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	currentView, ok := vs.view["current"]
	if ok == true {
		nextView, ok := vs.view["next"]

		backup := currentView.Backup
		if backup != "" && vs.expired(backup) == true {
			if ok == true {
				nextView.Backup = vs.getIdleServer()
				nextView.Viewnum = currentView.Viewnum + 1
				vs.view["next"] = nextView

				if vs.acknowledged == true {
					vs.view["current"] = vs.view["next"]
					vs.acknowledged = false
				}
			}
		}

		primary := currentView.Primary
		if primary != "" && vs.expired(primary) == true {
			if ok == true {
				nextView.Primary = nextView.Backup
				nextView.Backup = vs.getIdleServer()
				nextView.Viewnum = currentView.Viewnum + 1
				vs.view["next"] = nextView

				if vs.acknowledged == true {
					vs.view["current"] = vs.view["next"]
					vs.acknowledged = false
				}
			}
		}
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	vs.dead = 0
	vs.lastPingTime = make(map[string]time.Time)
	vs.view = make(map[string]View)

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
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
