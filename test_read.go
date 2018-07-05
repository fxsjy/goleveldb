package main
import (
    "fmt"
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
        value, gErr := db.Get([]byte(key), nil)
        if gErr != nil {
            panic(gErr)
        }
        if i % 10000 == 0 {
            fmt.Println(key, len(value))
        }
    }
    iter := db.NewIterator(nil, nil)
    defer iter.Release()
    j := 0
    for  iter.Next() {
        if j % 10000 == 0 {
            fmt.Println(string(iter.Key()), len(iter.Value()))
        }
        j++
    }
    c_err := db.Close()
    if c_err != nil {
        panic(c_err)
    }
}
