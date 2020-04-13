package mastercontrol

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	FILE_PREFIX      = "epaxos_message_"
	CHAN_BUFFER_SIZE = 200000
)

type PCTFile struct {
	messagePool    map[int]*Message
	enabled        []bool
	active         []bool
	stopChan       chan int
	dispatchChan   chan *Message
	numReplicas    int
	replicaIds     []int
	messageCounter int
	lock           *sync.Mutex
	params         map[string]string
	ackChan        chan int
	fileInterface  *FileInterface
}

func NewPCTFileController(numNodes int, configPath string) *PCTFile {

	configFile, err := ioutil.ReadFile(configPath)
	if err != nil {
		log.Fatal("Cannot read config for controller.")
		return nil
	}

	config := make(map[string]string)

	err = json.Unmarshal(configFile, &config)
	if err != nil {
		log.Fatal("Could not read config from config file for controller.")
	}

	ackChan := make(chan int, CHAN_BUFFER_SIZE)

	return &PCTFile{
		make(map[int]*Message),
		make([]bool, numNodes),
		make([]bool, numNodes),
		nil,
		nil,
		numNodes,
		make([]int, numNodes),
		0,
		new(sync.Mutex),
		config,
		ackChan,
		NewFileInterface(config["working_dir"], ackChan),
	}
}

func (pctFile *PCTFile) Init(
	params map[string]string,
	stopChan chan int,
	dispatchChan chan *Message,
) {
	pctFile.lock.Lock()
	defer pctFile.lock.Unlock()

	pctFile.stopChan = stopChan
	pctFile.dispatchChan = dispatchChan

	for i := 0; i < pctFile.numReplicas; i++ {
		pctFile.replicaIds[i] = i
		pctFile.active[i] = false
		pctFile.enabled[i] = true
	}
}

func (pctFile *PCTFile) ShallStart(replicaId int) bool {
	return pctFile.enabled[replicaId]
}

func (pctFile *PCTFile) NotifyMessage(message *Message) {
	pctFile.lock.Lock()
	defer pctFile.lock.Unlock()

	message.ID = pctFile.messageCounter
	pctFile.messagePool[pctFile.messageCounter] = message
	pctFile.messageCounter = pctFile.messageCounter + 1
	pctFile.fileInterface.WriteMessage(message)
}

func (pctFile *PCTFile) ackmonitor() {
	for messageid := range pctFile.ackChan {
		pctFile.lock.Lock()
		if message, ok := pctFile.messagePool[messageid]; ok {
			log.Printf("Acknowledging message with id %d", messageid)
			pctFile.dispatchChan <- message
			delete(pctFile.messagePool, messageid)
		}
		pctFile.lock.Unlock()
	}
	close(pctFile.dispatchChan)
}

func (pctFile *PCTFile) Run() {
	go pctFile.ackmonitor()
	pctFile.fileInterface.Run()
}

type FileInterface struct {
	workingDir  string
	newDir      string
	sendDir     string
	ackDir      string
	messageChan chan int
	stop        bool
}

func NewFileInterface(workingDir string, ackChan chan int) *FileInterface {

	newdir := filepath.Join(workingDir, "new")
	senddir := filepath.Join(workingDir, "send")
	ackdir := filepath.Join(workingDir, "ack")

	_ = os.MkdirAll(newdir, os.ModePerm)
	_ = os.MkdirAll(senddir, os.ModePerm)
	_ = os.MkdirAll(ackdir, os.ModePerm)

	return &FileInterface{
		workingDir,
		newdir,
		senddir,
		ackdir,
		ackChan,
		false,
	}
}

func (f *FileInterface) Run() {
	go f.monitoracks()
	go f.tempdispatcher()
}

func (f *FileInterface) tempdispatcher() {
	// Need to send files from send folder to ack folder blindly
	for !f.stop {
		files, err := ioutil.ReadDir(f.sendDir)
		if err == nil {
			for _, file := range files {
				if strings.HasPrefix(file.Name(), FILE_PREFIX) {
					oldname := filepath.Join(f.sendDir, file.Name())
					newname := filepath.Join(f.ackDir, file.Name())
					os.Rename(oldname, newname)
				}
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (f *FileInterface) monitoracks() {
	var err error = nil
	var files []os.FileInfo

	for !f.stop && err == nil {
		files, err = ioutil.ReadDir(f.ackDir)
		if err == nil {
			for _, file := range files {
				if strings.HasPrefix(file.Name(), FILE_PREFIX) {
					messageID, err := getMessageID(filepath.Join(f.ackDir, file.Name()))
					if err == nil && messageID >= 0 {
						log.Printf("Got acked file %s with message id %d", file.Name(), messageID)
						f.messageChan <- messageID
					}
				}
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(f.messageChan)
}

func getMessageID(filepath string) (int, error) {
	var messageid int = -1

	file, err := os.Open(filepath)
	if err != nil {
		return -1, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			return -1, errors.New("Error reading from message file")
		}
		if strings.HasPrefix(line, "eventId=") {
			t := strings.Split(strings.TrimSpace(line), "=")
			messageid, err = strconv.Atoi(t[1])
		}
	}
	_ = os.Remove(filepath)
	return messageid, err
}

func (f *FileInterface) WriteMessage(m *Message) {
	from := strconv.Itoa(m.From)
	to := strconv.Itoa(m.To)
	id := strconv.Itoa(m.ID)
	msgType := strconv.Itoa(int(m.MsgType))

	filename := FILE_PREFIX + from + "_" + to + "_" + id

	content := "eventId=" + id + "\n"
	content += "sender=" + from + "\n"
	content += "recv=" + to + "\n"
	content += "msgtype=" + msgType + "\n"

	go f.createAndCommitFile(filename, content)
}

func (f *FileInterface) createAndCommitFile(filename string, content string) {
	err := f.createFile(filename, content)
	if err != nil {
		return
	}
	f.commitFile(filename)
}

func (f *FileInterface) commitFile(filename string) error {
	oldpath := filepath.Join(f.newDir, filename)
	newpath := filepath.Join(f.sendDir, filename)
	return os.Rename(oldpath, newpath)
}

func (f *FileInterface) createFile(filename string, content string) error {
	log.Printf("Creating file %s", filename)

	file, err := os.Create(filepath.Join(f.newDir, filename))
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	writer.WriteString(content)
	return writer.Flush()
}
