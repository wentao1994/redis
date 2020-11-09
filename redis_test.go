package redis

import (
	"fmt"
	"log"
	"regexp"
	"testing"
	"time"
)

func equalInt(a, b int, msg string) {
	if a != b {
		log.Panicln(msg)
	}
}

func equalError(a, b error, msg string) {
	if a != b {
		log.Panicln(msg)
	}
}

func notEqualError(a, b error, msg string) {
	if a == b {
		log.Panicln(msg)
	}
}

func equalString(a, b, msg string) {
	if a != b {
		log.Panicln(msg)
	}
}

func equalBool(a, b bool, msg string) {
	if a != b {
		log.Panicln(msg)
	}
}

func equalRegexp(pattern, str, msg string) {
	match, err := regexp.Match(pattern, []byte(str))
	if err != nil || !match {
		log.Panicln(msg)
	}
}

var testTarget = "10.20.1.20:7300,10.20.1.20:7301"

func TestInitOldPool(t *testing.T) {
	fmt.Println("==============test init old pool==============")

	cliPool, err := NewClient(testTarget)
	if err != nil {
		t.Fatal(err)
	}
	_, err = cliPool.Del("foo", "testfoo")
	equalError(nil, err, "del failed")

	val_s, err := cliPool.Set("foo", "bar", 0)
	equalString(val_s, "OK", "set failed")
	equalError(nil, err, "set failed")

	val_s, err = cliPool.Get("foo")
	equalString(val_s, "bar", "get failed")
	equalError(nil, err, "get failed")

	val_i, err := cliPool.IncrBy("testfoo", 3)
	equalInt(val_i, 3, "incrby failed")
	equalError(nil, err, "incrby failed")

	cliPool.Close()
}

func TestInitNewPool(t *testing.T) {
	fmt.Println("==============test init new pool==============")
	cliPool, err := NewClient(testTarget,
		ConnectTimeout(1000*time.Millisecond),
		ReadTimeout(500*time.Millisecond),
		WriteTimeout(500*time.Millisecond),
		IdleTimeout(60*time.Second),
		MaxActive(50),
		MaxIdle(3),
		WaitConn(true))
	if err != nil {
		t.Fatal(err)
	}
	_, err = cliPool.Del("foo", "testfoo")
	equalError(nil, err, "del failed")

	val_s, err := cliPool.Set("foo", "bar", 0)
	equalString(val_s, "OK", "set failed")
	equalError(nil, err, "set failed")

	val_s, err = cliPool.Get("foo")
	equalString(val_s, "bar", "get failed")
	equalError(nil, err, "get failed")

	val_i, err := cliPool.IncrBy("testfoo", 3)
	equalInt(val_i, 3, "incrby failed")
	equalError(nil, err, "incrby failed")

	cliPool.Close()
}

func TestConnectTimeout(t *testing.T) {
	fmt.Println("==============test connect timeout==============")

	cliPool, err := NewClient(testTarget,
		ConnectTimeout(1*time.Nanosecond),
		ReadTimeout(500*time.Millisecond),
		WriteTimeout(500*time.Millisecond),
		IdleTimeout(60*time.Second),
		MaxActive(50),
		MaxIdle(3),
		WaitConn(true))
	if err != nil {
		t.Fatal(err)
	}

	_, err = cliPool.Set("foo", "bar", 0)
	equalRegexp("dial.*i/o timeout", err.Error(), "connect timeout failed")

	cliPool.Close()

}

func TestReadTimeout(t *testing.T) {
	fmt.Println("==============test read timeout==============")

	cliPool, err := NewClient(testTarget,
		ConnectTimeout(1000*time.Millisecond),
		ReadTimeout(5*time.Nanosecond),
		WriteTimeout(500*time.Millisecond),
		IdleTimeout(60*time.Second),
		MaxActive(50),
		MaxIdle(3),
		WaitConn(true))
	if err != nil {
		t.Fatal(err)
	}

	_, err = cliPool.Set("foo", "bar", 0)
	equalRegexp("read.*i/o timeout", err.Error(), "read timeout failed")

	_, err = cliPool.Get("foo")
	equalRegexp("read.*i/o timeout", err.Error(), "read timeout failed")

	cliPool.Close()

}

func TestWriteTimeout(t *testing.T) {
	fmt.Println("==============test write timeout==============")

	cliPool, err := NewClient(testTarget,
		ConnectTimeout(1000*time.Millisecond),
		ReadTimeout(500*time.Millisecond),
		WriteTimeout(5*time.Nanosecond),
		IdleTimeout(60*time.Second),
		MaxActive(50),
		MaxIdle(3),
		WaitConn(true))
	if err != nil {
		t.Fatal(err)
	}

	_, err = cliPool.Set("foo", "bar", 0)
	equalRegexp("write.*i/o timeout", err.Error(), "write timeout failed")

	_, err = cliPool.Get("foo")
	equalRegexp("write.*i/o timeout", err.Error(), "write timeout failed")

	cliPool.Close()

}

func TestMaxActive(t *testing.T) {
	fmt.Println("==============test max active==============")

	cliPool, err := NewClient(testTarget,
		ConnectTimeout(1000*time.Millisecond),
		ReadTimeout(500*time.Millisecond),
		WriteTimeout(500*time.Millisecond),
		IdleTimeout(1*time.Second),
		MaxActive(1),
		MaxIdle(1),
		WaitConn(false))
	if err != nil {
		t.Fatal(err)
	}
	num := 5

	for i := 0; i < num; i++ {
		go func() {
			_, err := cliPool.Set("foo", "bar", 0)
			if err != nil {
				fmt.Println("set fail! err:", err)
			}

			val, err := cliPool.Get("foo")
			if err != nil {
				fmt.Println("get fail! err:", err)
			} else {
				fmt.Println(val)
			}
		}()
	}

	select {
	case <-time.After(time.Millisecond * 500):
	}
	cliPool.Close()
}

func TestWait(t *testing.T) {
	fmt.Println("==============test wait==============")

	cliPool, err := NewClient(testTarget,
		ConnectTimeout(1000*time.Millisecond),
		ReadTimeout(500*time.Millisecond),
		WriteTimeout(500*time.Millisecond),
		IdleTimeout(1*time.Second),
		MaxActive(1),
		MaxIdle(1),
		WaitConn(true))
	if err != nil {
		t.Fatal(err)
	}
	num := 5

	for i := 0; i < num; i++ {
		go func() {
			val_s, err := cliPool.Set("foo", "bar", 0)
			equalString(val_s, "OK", "set failed")
			equalError(nil, err, "wait failed")

			val_s, err = cliPool.Get("foo")
			equalString(val_s, "bar", "get failed")
			equalError(nil, err, "wait failed")
		}()
	}

	select {
	case <-time.After(time.Millisecond * 500):
	}
	cliPool.Close()
}

func TestReadStale(t *testing.T) {
	fmt.Println("==============test read stale==============")
	cliPool, err := NewClient("10.20.1.20:7300,127.0.0.1:44444")
	if err != nil {
		t.Fatal(err)
	}

	val_s, err := cliPool.Set("foo", "bar", 0)
	equalString(val_s, "OK", "set failed")
	equalError(nil, err, "set failed")

	val_s, err = cliPool.Get("foo")
	notEqualError(nil, err, "get failed")

	cliPool.SetReadStale(false)

	val_s, err = cliPool.Get("foo")
	equalString(val_s, "bar", "get failed")
	equalError(nil, err, "get failed")

	cliPool.Close()
}
