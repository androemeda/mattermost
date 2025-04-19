package elasticsearch

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/mattermost/mattermost/server/public/model"
)

const (
	PostIndexMapping = `{
		"settings": {
			"index": {
				"number_of_shards": %d,
				"number_of_replicas": %d
			},
			"analysis": {
				"analyzer": {
					"custom_analyzer": {
						"type": "custom",
						"tokenizer": "standard",
						"filter": ["lowercase", "stop", "snowball"]
					}
				}
			}
		},
		"mappings": {
			"properties": {
				"message": {
					"type": "text",
					"analyzer": "custom_analyzer",
					"search_analyzer": "custom_analyzer"
				},
				"channel_id": {
					"type": "keyword"
				},
				"team_id": {
					"type": "keyword"
				},
				"user_id": {
					"type": "keyword"
				},
				"create_at": {
					"type": "date",
					"format": "epoch_millis"
				},
				"update_at": {
					"type": "date",
					"format": "epoch_millis"
				},
				"delete_at": {
					"type": "date",
					"format": "epoch_millis"
				},
				"is_pinned": {
					"type": "boolean"
				},
				"hashtags": {
					"type": "text",
					"analyzer": "custom_analyzer",
					"search_analyzer": "custom_analyzer"
				}
			}
		}
	}`

	UserIndexMapping = `{
		"settings": {
			"index": {
				"number_of_shards": %d,
				"number_of_replicas": %d
			},
			"analysis": {
				"analyzer": {
					"custom_analyzer": {
						"type": "custom",
						"tokenizer": "standard",
						"filter": ["lowercase", "stop"]
					}
				}
			}
		},
		"mappings": {
			"properties": {
				"username": {
					"type": "text",
					"analyzer": "custom_analyzer",
					"search_analyzer": "custom_analyzer",
					"fields": {
						"keyword": {
							"type": "keyword"
						}
					}
				},
				"first_name": {
					"type": "text",
					"analyzer": "custom_analyzer",
					"search_analyzer": "custom_analyzer"
				},
				"last_name": {
					"type": "text",
					"analyzer": "custom_analyzer",
					"search_analyzer": "custom_analyzer"
				},
				"nickname": {
					"type": "text",
					"analyzer": "custom_analyzer",
					"search_analyzer": "custom_analyzer"
				},
				"email": {
					"type": "keyword"
				},
				"create_at": {
					"type": "date",
					"format": "epoch_millis"
				},
				"update_at": {
					"type": "date",
					"format": "epoch_millis"
				},
				"delete_at": {
					"type": "date",
					"format": "epoch_millis"
				},
				"teams_ids": {
					"type": "keyword"
				},
				"channels_ids": {
					"type": "keyword"
				}
			}
		}
	}`

	FileIndexMapping = `{
		"settings": {
			"index": {
				"number_of_shards": %d,
				"number_of_replicas": %d
			},
			"analysis": {
				"analyzer": {
					"custom_analyzer": {
						"type": "custom",
						"tokenizer": "standard",
						"filter": ["lowercase", "stop"]
					}
				}
			}
		},
		"mappings": {
			"properties": {
				"name": {
					"type": "text",
					"analyzer": "custom_analyzer",
					"search_analyzer": "custom_analyzer",
					"fields": {
						"keyword": {
							"type": "keyword"
						}
					}
				},
				"content": {
					"type": "text",
					"analyzer": "custom_analyzer",
					"search_analyzer": "custom_analyzer"
				},
				"extension": {
					"type": "keyword"
				},
				"content_type": {
					"type": "keyword"
				},
				"create_at": {
					"type": "date",
					"format": "epoch_millis"
				},
				"update_at": {
					"type": "date",
					"format": "epoch_millis"
				},
				"delete_at": {
					"type": "date",
					"format": "epoch_millis"
				},
				"size": {
					"type": "long"
				},
				"channel_id": {
					"type": "keyword"
				},
				"user_id": {
					"type": "keyword"
				}
			}
		}
	}`
)

func (e *ElasticsearchEngine) createIndex(indexName string, mapping string, shards, replicas int) *model.AppError {
	exists, err := e.Client.Indices.Exists([]string{indexName})
	if err != nil {
		return model.NewAppError("ElasticsearchEngine.createIndex", "searchengine.elasticsearch.create_index.error", nil, err.Error(), http.StatusInternalServerError)
	}
	defer exists.Body.Close()

	if exists.StatusCode == http.StatusOK {
		return nil
	}

	mappingJSON := json.RawMessage([]byte(mapping))
	res, err := e.Client.Indices.Create(
		indexName,
		e.Client.Indices.Create.WithBody(strings.NewReader(fmt.Sprintf(string(mappingJSON), shards, replicas))),
	)
	if err != nil {
		return model.NewAppError("ElasticsearchEngine.createIndex", "searchengine.elasticsearch.create_index.error", nil, err.Error(), http.StatusInternalServerError)
	}
	defer res.Body.Close()

	if res.IsError() {
		return model.NewAppError("ElasticsearchEngine.createIndex", "searchengine.elasticsearch.create_index.error", nil, res.String(), http.StatusInternalServerError)
	}

	return nil
}

func (e *ElasticsearchEngine) createIndexes() *model.AppError {
	if err := e.createIndex(PostIndex, PostIndexMapping, *e.Config.ElasticsearchSettings.PostIndexShards, *e.Config.ElasticsearchSettings.PostIndexReplicas); err != nil {
		return err
	}

	if err := e.createIndex(UserIndex, UserIndexMapping, *e.Config.ElasticsearchSettings.UserIndexShards, *e.Config.ElasticsearchSettings.UserIndexReplicas); err != nil {
		return err
	}

	if err := e.createIndex(FileIndex, FileIndexMapping, *e.Config.ElasticsearchSettings.PostIndexShards, *e.Config.ElasticsearchSettings.PostIndexReplicas); err != nil {
		return err
	}

	return nil
}