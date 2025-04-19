package searchengine

import (
	"github.com/mattermost/mattermost-server/v6/model"
)

type Broker struct {
	ElasticsearchEngine SearchEngineInterface
	BleveEngine         SearchEngineInterface
	cfg                 *model.Config
}

func (seb *Broker) RegisterElasticsearchEngine(es SearchEngineInterface) {
	seb.ElasticsearchEngine = es
}

func (seb *Broker) RegisterBleveEngine(be SearchEngineInterface) {
	seb.BleveEngine = be
}

func (seb *Broker) GetActiveEngines() []SearchEngineInterface {
	engines := []SearchEngineInterface{}
	if seb.ElasticsearchEngine != nil && seb.ElasticsearchEngine.IsActive() {
		engines = append(engines, seb.ElasticsearchEngine)
	}
	if seb.BleveEngine != nil && seb.BleveEngine.IsActive() && seb.BleveEngine.IsIndexingEnabled() {
		engines = append(engines, seb.BleveEngine)
	}
	return engines
}

func (seb *Broker) ActiveEngine() string {
	activeEngines := seb.GetActiveEngines()
	if len(activeEngines) > 0 {
		return activeEngines[0].GetName()
	}
	if *seb.cfg.SqlSettings.DisableDatabaseSearch {
		return "none"
	}
	return "database"
}