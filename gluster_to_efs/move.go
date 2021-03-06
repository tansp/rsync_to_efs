package main

import (
	"gluster_to_efs/conf"

	log "github.com/tansp/glog"
	"flag"

	efsconf "github.com/tansp/go-sdk/conf"
	"github.com/tansp/go-sdk/ecloud"
	"github.com/tansp/go-sdk/ecloudcli"

	"os"
	//	libpath "path"
	"path/filepath"
	"runtime"
	//	"strings"
	"sync"
	"time"
)

var (
	configFile string
)

func init() {
	flag.StringVar(&configFile, "c", "./move.toml", " set move config file path")
}

type Copy_file struct {
	c   *conf.Config
	l   *sync.RWMutex
	gos int
}

func (C *Copy_file) file_upload(bucket, filename, filepath string, filsize int64) {
	var (
		err error
	)
	c := ecloud.New(0, nil)
	// 设置上传的策略
	policy := &ecloud.PutPolicy{
		Scope: bucket,
		//设置Token过期时间
		Expires:    14400,
		InsertOnly: 0,
	}
	// 生成一个上传token
	token := c.MakeUptoken(policy)
	// 构建一个uploader
	zone := 0
	uploader := ecloudcli.NewUploader(zone, nil)
	log.Statis("%s %s %s %d ", bucket, filename, filepath, filsize)
	if filsize > 4*1024*1024 {
		err = uploader.RputFile(nil, nil, token, filename, filepath, nil)
	} else {
		err = uploader.PutFile(nil, nil, token, filename, filepath, nil)
	}
	if err != nil {
		log.Errorf("%s	%s	%s	%d	%v", bucket, filename, filepath, filsize, err)
		log.Statis("%s	%s	%s	%d	%d", bucket, filename, filepath, filsize, 400)
	} else {
		//log.Statis("%s %s %s %d %d", bucket, filename, filepath, filsize, 200)
		log.Infof("%s	%s	%s	%d	%d", bucket, filename, filepath, filsize, 200)
	}
	C.l.RLock()
	C.gos--
	C.l.RUnlock()

}

func (C *Copy_file) file_list_func(path string, f os.FileInfo, err error) error {
	if f == nil {
		return err
	}
	if f.IsDir() {
		return nil
	}

	bucket := C.c.Bucketname
	filelen := 0
	if string(path[len(C.c.CopyPath)]) == "/" {
		filelen = len(C.c.CopyPath) + 1
		//log.Errorf("come %s", string(path[len(C.c.CopyPath)]))
	} else {
		filelen = len(C.c.CopyPath)
	}
	filename := path[filelen:]
	C.l.RLock()
	gos := C.gos

	if gos < C.c.Threads {
		C.gos++
		C.l.RUnlock()
		go C.file_upload(bucket, filename, path, int64(f.Size()))
	} else {
		C.gos++
		C.l.RUnlock()
		C.file_upload(bucket, filename, path, int64(f.Size()))
	}
	return nil
}

func main() {
	var (
		c   *conf.Config
		err error
		cf  *Copy_file
	)
	flag.Parse()
	defer log.Flush()
	if c, err = conf.NewConfig(configFile); err != nil {
		log.Errorf("NewConfig(\"%s\") error(%v)", configFile, err)
		panic(err)
	}
	runtime.GOMAXPROCS(runtime.NumCPU())
	cf = &Copy_file{}
	cf.c = c
	cf.l = new(sync.RWMutex)
	efsconf.ACCESS_KEY = c.UserAk
	efsconf.SECRET_KEY = c.UserSk
	efsconf.Zones[0].UpHosts = append(efsconf.Zones[0].UpHosts, c.UploadHttpaddr)

	err = filepath.Walk(c.CopyPath, cf.file_list_func)
	for {
		if cf.gos == 0 {
			break
		} else {
			time.Sleep(1)
		}
	}

	log.Errorf("COPY over %v", err)
}
