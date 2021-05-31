package tailfile

import (
	"Log_Collection/common"
	"github.com/sirupsen/logrus"
)

type tailTaskManage struct {
	// key: path
	tailTaskMap      map[string]*tailTask
	collectEntryList []common.CollectEntry
	confChan         chan []common.CollectEntry
}

var (
	taskManageObj *tailTaskManage
)

// called in the main func
// There are many configs parts in allconf. Create a tailObj for every config part
func Init(allConf []common.CollectEntry) (err error) {
	// init a taskManageObj
	taskManageObj = &tailTaskManage{
		tailTaskMap:      make(map[string]*tailTask, 20),
		collectEntryList: allConf,
		// init blocked channel(wait until new configs come)
		confChan: make(chan []common.CollectEntry),
	}
	for _, conf := range allConf {
		task := newTailTask(conf.Path, conf.Topic)
		err := task.Init()
		if err != nil {
			logrus.Errorf("tailfile: create tailObj for path:%s failed,err:%v", conf.Path, err)
			continue
		}
		logrus.Infof("create a tail task for path:%v success!", conf.Path)
		taskManageObj.tailTaskMap[task.path] = task
		go task.run()
	}
	go taskManageObj.watch() // wait for new configs
	return
}

func (tm *tailTaskManage) watch() {
	for {
		// wait for new config from etcd
		newConfig := <-tm.confChan
		logrus.Infof("get new config from etcd,start managing tailTask! new config:%v", newConfig)
		for _, newconf := range newConfig {
			// !!! manage configs sent from etcd
			// m1. do nothing if "config" has existed
			if tm.isExist(newconf) {
				continue
			}
			// m2. create a new tailTask if "config" is new
			task := newTailTask(newconf.Path, newconf.Topic)
			err := task.Init()
			if err != nil {
				logrus.Errorf("tailfile: create tailObj for path:%s failed,err:%v", newconf.Path, err)
				continue
			}
			logrus.Infof("create a tail task for path:%v success!", newconf.Path)
			tm.tailTaskMap[task.path] = task
			go task.run()
		}
		// m3. stop a tailTask if "config" has deleted
		// find config which exists in tailTaskMap but not exists in newConfig
		// Then stop the config
		for key, task := range tm.tailTaskMap {
			var isfound bool
			for _, newConf := range newConfig {
				if key == newConf.Path {
					isfound = true
					// delete task from taskMap
					delete(tm.tailTaskMap, key)
					break
				}
			}
			if !isfound {
				logrus.Infof("The task should stop,task path:%v", task.path)
				task.cancel()
			}
		}
	}
}

func (tm *tailTaskManage) isExist(config common.CollectEntry) bool {
	_, ok := tm.tailTaskMap[config.Path]
	return ok
}

func SendNewConfig(newConfig []common.CollectEntry) {
	taskManageObj.confChan <- newConfig
}
