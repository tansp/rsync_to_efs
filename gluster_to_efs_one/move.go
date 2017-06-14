package main

import (
	"gluster_to_efs/conf"

	log "efs/log/glog"
	"flag"

	efsconf "ecloud_gosdk.v1/conf"
	"ecloud_gosdk.v1/ecloud"
	"ecloud_gosdk.v1/ecloudcli"

	"os"
	//	libpath "path"
	//	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

var (
	configFile string
)

func init() {
	flag.StringVar(&configFile, "c", "./move.toml", " set move config file path")
}

func file_upload(bucket, filename, filepath string, filsize int64) {
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

}

func main() {
	var (
		c   *conf.Config
		err error
	)
	flag.Parse()
	defer log.Flush()
	if c, err = conf.NewConfig(configFile); err != nil {
		log.Errorf("NewConfig(\"%s\") error(%v)", configFile, err)
		panic(err)
	}
	runtime.GOMAXPROCS(runtime.NumCPU())

	efsconf.ACCESS_KEY = c.UserAk
	efsconf.SECRET_KEY = c.UserSk
	efsconf.Zones[0].UpHosts = append(efsconf.Zones[0].UpHosts, c.UploadHttpaddr)

	path := os.Args[1]
	filelen := 0
	if !strings.HasPrefix(path, c.CopyPath) {
		log.Errorf("have no path %s", path)
		return
	}
	if string(path[len(c.CopyPath)]) == "/" {
		filelen = len(c.CopyPath) + 1
		//log.Errorf("come %s", string(path[len(C.c.CopyPath)]))
	} else {
		filelen = len(c.CopyPath)
	}
	filename := path[filelen:]
	filesize, _ := strconv.ParseInt(os.Args[2], 10, 64)
	//log.Errorf("name=%s name=%s path = %s size=%d", c.Bucketname, filename, path, filesize)
	file_upload(c.Bucketname, filename, path, filesize)
}
