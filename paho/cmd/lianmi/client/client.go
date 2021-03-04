package main

import (
	"bufio"
	"context"
	"time"

	// "errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/eclipse/paho.golang/paho"
	"github.com/gomodule/redigo/redis"
)

func main() {

	redisConn, err := redis.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		log.Fatalln(err)
		return
	}

	defer redisConn.Close()
	var localUserName string
	var localDeviceID string
	localUserName, _ = redis.String(redisConn.Do("GET", "LocalUserName"))
	localDeviceID, _ = redis.String(redisConn.Do("GET", "LocalDeviceID"))
	if localUserName == "" {
		log.Println("localUserName is  empty")
		return
	}
	if localDeviceID == "" {
		log.Println("localDeviceID is empty")
		return
	}
	// responseTopic := fmt.Sprintf("lianmi/cloud/%s", localDeviceID)

	//从本地redis里获取jwtToken，注意： 在auth模块的登录，登录成功后，需要写入，这里则读取
	key := fmt.Sprintf("jwtToken:%s", localUserName)
	jwtToken, err := redis.String(redisConn.Do("GET", key))
	if err != nil {
		log.Println("Redis GET jwtToken:{localUserName}", err)
		return
	}
	if jwtToken == "" {
		return
	}

	taskId, _ := redis.Int(redisConn.Do("INCR", fmt.Sprintf("taksID:%s", localUserName)))
	taskIdStr := fmt.Sprintf("%d", taskId)

	stdin := bufio.NewReader(os.Stdin)
	// hostname, _ := os.Hostname()

	server := flag.String("server", "127.0.0.1:1883", "The full URL of the MQTT server to connect to")
	topic := flag.String("topic", "lianmi/cloud/dispatcher", "Topic to publish the messages on")
	qos := flag.Int("qos", 2, "The QoS to send the messages at")
	retained := flag.Bool("retained", false, "Are the messages sent with the retained flag")
	clientid := flag.String("clientid", "client", "A clientid for the connection")
	username := flag.String("username", "", "A username to authenticate to the MQTT server")
	password := flag.String("password", "", "Password to match username")
	flag.Parse()

	conn, err := net.Dial("tcp", *server)
	if err != nil {
		log.Fatalf("Failed to connect to %s: %s", *server, err)
	}

	c := paho.NewClient(paho.ClientConfig{
		Conn: conn,
	})

	cp := &paho.Connect{
		KeepAlive:  30,
		ClientID:   *clientid,
		CleanStart: true,
		Username:   *username,
		Password:   []byte(*password),
	}

	if *username != "" {
		cp.UsernameFlag = true
	}
	if *password != "" {
		cp.PasswordFlag = true
	}

	log.Println(cp.UsernameFlag, cp.PasswordFlag)

	ca, err := c.Connect(context.Background(), cp)
	if err != nil {
		log.Fatalln(err)
	}
	if ca.ReasonCode != 0 {
		log.Fatalf("Failed to connect to %s : %d - %s", *server, ca.ReasonCode, ca.Properties.ReasonString)
	}

	fmt.Printf("Connected to %s\n", *server)

	ic := make(chan os.Signal, 1)
	signal.Notify(ic, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ic
		fmt.Println("signal received, exiting")
		if c != nil {
			d := &paho.Disconnect{ReasonCode: 0}
			c.Disconnect(d)
		}
		os.Exit(0)
	}()

	for {
		message, err := stdin.ReadString('\n')
		if err == io.EOF {
			os.Exit(0)
		}
		props := &paho.PublishProperties{}

		// props.ResponseTopic = responseTopic
		props.User = props.User.Add("jwtToken", jwtToken)
		props.User = props.User.Add("deviceId", localDeviceID)
		props.User = props.User.Add("businessType", "1")
		props.User = props.User.Add("businessSubType", "1")
		props.User = props.User.Add("taskId", taskIdStr)
		props.User = props.User.Add("code", "200")
		props.User = props.User.Add("errormsg", "")

		pb := &paho.Publish{
			Topic:      *topic,
			QoS:        byte(*qos),
			Retain:     *retained,
			Payload:    []byte(message),
			Properties: props,
		}

		// for i := 0; i < 10; i++ {

		// }
		for range time.Tick(time.Millisecond * 100) {
			// fmt.Println("Hello TigerwolfC")
			if _, err = c.Publish(context.Background(), pb); err != nil {
				log.Println("error sending message:", err)
				continue
			}
		}

		log.Println("sent")
	}
}
