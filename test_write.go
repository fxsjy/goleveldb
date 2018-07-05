package main
import (
    "fmt"
    "strings"
    "github.com/syndtr/goleveldb/leveldb"
    "github.com/syndtr/goleveldb/leveldb/opt"
)

func main() {
    var myoption opt.Options
    myoption.DataPaths = []string{"./disks/disk1/", "./disks/disk2", "./disks/disk3"}
    db, err := leveldb.OpenFile("./data", &myoption)
    if err != nil {
        panic(err)
    }
    for i :=0 ; i<100000; i++ {
        key := fmt.Sprintf("Key_%08d", i)
        value := strings.Repeat("x", 1024)
        p_err := db.Put([]byte(key), []byte(value), nil)
        if p_err != nil {
            panic(p_err)
        }
        if i % 10000 == 0 {
            fmt.Println(i)
        }
    }
    c_err := db.Close()
    if c_err != nil {
        panic(c_err)
    }
}
