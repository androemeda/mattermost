package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/shared/mlog"
	"github.com/mattermost/mattermost/server/public/shared/request"
)

const (
	EngineName = "elasticsearch"
	PostIndex  = "posts"
	UserIndex  = "users"
	FileIndex  = "files"
)

type ElasticsearchEngine struct {
	Client     *elasticsearch.Client
	Config     *model.Config
	ConfigMut  sync.RWMutex
	ready      bool
	indexSync  bool
}

func NewElasticsearchEngine(cfg *model.Config) *ElasticsearchEngine {
	return &ElasticsearchEngine{
		Config:    cfg,
		ready:    false,
		indexSync: false,
	}
}

func (e *ElasticsearchEngine) Start() *model.AppError {
	if !*e.Config.ElasticsearchSettings.EnableIndexing {
		return nil
	}

	cfg := elasticsearch.Config{
		Addresses: []string{*e.Config.ElasticsearchSettings.ConnectionUrl},
		Username:  *e.Config.ElasticsearchSettings.Username,
		Password:  *e.Config.ElasticsearchSettings.Password,
	}

	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return model.NewAppError("ElasticsearchEngine.Start", "searchengine.elasticsearch.start_failed", nil, err.Error(), http.StatusInternalServerError)
	}

	e.Client = client
	e.ready = true
	return nil
}

func (e *ElasticsearchEngine) Stop() *model.AppError {
	e.ready = false
	return nil
}

func (e *ElasticsearchEngine) IsActive() bool {
	return e.ready
}

func (e *ElasticsearchEngine) IsIndexingEnabled() bool {
	return *e.Config.ElasticsearchSettings.EnableIndexing
}

func (e *ElasticsearchEngine) IsSearchEnabled() bool {
	return *e.Config.ElasticsearchSettings.EnableSearching
}

func (e *ElasticsearchEngine) IsAutocompletionEnabled() bool {
	return *e.Config.ElasticsearchSettings.EnableAutocomplete
}

func (e *ElasticsearchEngine) IsIndexingSync() bool {
	return e.indexSync
}

func (e *ElasticsearchEngine) GetName() string {
	return EngineName
}

func (e *ElasticsearchEngine) GetVersion() int {
	return 1
}

func (e *ElasticsearchEngine) GetFullVersion() string {
	return "1.0.0"
}

func (e *ElasticsearchEngine) GetPlugins() []string {
	return []string{}
}

func (e *ElasticsearchEngine) UpdateConfig(cfg *model.Config) {
	e.ConfigMut.Lock()
	defer e.ConfigMut.Unlock()
	e.Config = cfg
}

func (e *ElasticsearchEngine) TestConfig(rctx request.CTX, cfg *model.Config) *model.AppError {
	if !*cfg.ElasticsearchSettings.EnableIndexing {
		return nil
	}

	escfg := elasticsearch.Config{
		Addresses: []string{*cfg.ElasticsearchSettings.ConnectionUrl},
		Username:  *cfg.ElasticsearchSettings.Username,
		Password:  *cfg.ElasticsearchSettings.Password,
	}

	client, err := elasticsearch.NewClient(escfg)
	if err != nil {
		return model.NewAppError("ElasticsearchEngine.TestConfig", "searchengine.elasticsearch.test_config_failed", nil, err.Error(), http.StatusInternalServerError)
	}

	info, err := client.Info()
	if err != nil {
		return model.NewAppError("ElasticsearchEngine.TestConfig", "searchengine.elasticsearch.test_config_failed", nil, err.Error(), http.StatusInternalServerError)
	}
	defer info.Body.Close()

	return nil
}

func (e *ElasticsearchEngine) IndexPost(post *model.Post, teamId string) *model.AppError {
	if !e.IsActive() {
		return nil
	}

	document := map[string]interface{}{
		"message":    post.Message,
		"channel_id": post.ChannelId,
		"team_id":    teamId,
		"user_id":    post.UserId,
		"create_at":  post.CreateAt,
		"update_at":  post.UpdateAt,
		"delete_at":  post.DeleteAt,
		"is_pinned":  post.IsPinned,
		"hashtags":   post.Hashtags,
	}

	req := esapi.IndexRequest{
		Index:      PostIndex,
		DocumentID: post.Id,
		Body:       strings.NewReader(toJSON(document)),
		Refresh:    "true",
	}

	res, err := req.Do(context.Background(), e.Client)
	if err != nil {
		return model.NewAppError("ElasticsearchEngine.IndexPost", "searchengine.elasticsearch.index_post.error", nil, err.Error(), http.StatusInternalServerError)
	}
	defer res.Body.Close()

	if res.IsError() {
		return model.NewAppError("ElasticsearchEngine.IndexPost", "searchengine.elasticsearch.index_post.error", nil, res.String(), http.StatusInternalServerError)
	}

	return nil
}

func (e *ElasticsearchEngine) SearchPosts(channels model.ChannelList, searchParams []*model.SearchParams, page, perPage int) ([]string, model.PostSearchMatches, *model.AppError) {
	if !e.IsActive() {
		return nil, nil, nil
	}

	var query map[string]interface{}
	channelIds := make([]string, len(channels))
	for i, channel := range channels {
		channelIds[i] = channel.Id
	}

	query = map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"terms": map[string]interface{}{
							"channel_id": channelIds,
						},
					},
				},
			},
		},
		"from": page * perPage,
		"size": perPage,
		"sort": []map[string]interface{}{
			{
				"create_at": map[string]interface{}{
					"order": "desc",
				},
			},
		},
	}

	// Add search terms
	for _, params := range searchParams {
		if params.Terms != "" {
			query["query"].(map[string]interface{})["bool"].(map[string]interface{})["must"] = append(
				query["query"].(map[string]interface{})["bool"].(map[string]interface{})["must"].([]map[string]interface{}),
				map[string]interface{}{
					"match": map[string]interface{}{
						"message": params.Terms,
					},
				},
			)
		}
	}

	res, err := e.Client.Search(
		e.Client.Search.WithIndex(PostIndex),
		e.Client.Search.WithBody(strings.NewReader(toJSON(query))),
	)
	if err != nil {
		return nil, nil, model.NewAppError("ElasticsearchEngine.SearchPosts", "searchengine.elasticsearch.search_posts.error", nil, err.Error(), http.StatusInternalServerError)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, nil, model.NewAppError("ElasticsearchEngine.SearchPosts", "searchengine.elasticsearch.search_posts.error", nil, res.String(), http.StatusInternalServerError)
	}

	var result struct {
		Hits struct {
			Total struct {
				Value int `json:"value"`
			} `json:"total"`
			Hits []struct {
				ID     string                 `json:"_id"`
				Source map[string]interface{} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, nil, model.NewAppError("ElasticsearchEngine.SearchPosts", "searchengine.elasticsearch.search_posts.parse_error", nil, err.Error(), http.StatusInternalServerError)
	}

	postIds := make([]string, len(result.Hits.Hits))
	matches := make(model.PostSearchMatches)

	for i, hit := range result.Hits.Hits {
		postIds[i] = hit.ID
		matches[hit.ID] = []string{} // We don't support term highlighting yet
	}

	return postIds, matches, nil
}

func (e *ElasticsearchEngine) DeletePost(post *model.Post) *model.AppError {
	if !e.IsActive() {
		return nil
	}

	req := esapi.DeleteRequest{
		Index:      PostIndex,
		DocumentID: post.Id,
		Refresh:    "true",
	}

	res, err := req.Do(context.Background(), e.Client)
	if err != nil {
		return model.NewAppError("ElasticsearchEngine.DeletePost", "searchengine.elasticsearch.delete_post.error", nil, err.Error(), http.StatusInternalServerError)
	}
	defer res.Body.Close()

	if res.IsError() && res.StatusCode != http.StatusNotFound {
		return model.NewAppError("ElasticsearchEngine.DeletePost", "searchengine.elasticsearch.delete_post.error", nil, res.String(), http.StatusInternalServerError)
	}

	return nil
}

func (e *ElasticsearchEngine) DeleteChannelPosts(rctx request.CTX, channelID string) *model.AppError {
	if !e.IsActive() {
		return nil
	}

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				"channel_id": channelID,
			},
		},
	}

	res, err := e.Client.DeleteByQuery(
		[]string{PostIndex},
		strings.NewReader(toJSON(query)),
		e.Client.DeleteByQuery.WithRefresh(true),
	)
	if err != nil {
		return model.NewAppError("ElasticsearchEngine.DeleteChannelPosts", "searchengine.elasticsearch.delete_channel_posts.error", nil, err.Error(), http.StatusInternalServerError)
	}
	defer res.Body.Close()

	if res.IsError() {
		return model.NewAppError("ElasticsearchEngine.DeleteChannelPosts", "searchengine.elasticsearch.delete_channel_posts.error", nil, res.String(), http.StatusInternalServerError)
	}

	return nil
}

func (e *ElasticsearchEngine) DeleteUserPosts(rctx request.CTX, userID string) *model.AppError {
	if !e.IsActive() {
		return nil
	}

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				"user_id": userID,
			},
		},
	}

	res, err := e.Client.DeleteByQuery(
		[]string{PostIndex},
		strings.NewReader(toJSON(query)),
		e.Client.DeleteByQuery.WithRefresh(true),
	)
	if err != nil {
		return model.NewAppError("ElasticsearchEngine.DeleteUserPosts", "searchengine.elasticsearch.delete_user_posts.error", nil, err.Error(), http.StatusInternalServerError)
	}
	defer res.Body.Close()

	if res.IsError() {
		return model.NewAppError("ElasticsearchEngine.DeleteUserPosts", "searchengine.elasticsearch.delete_user_posts.error", nil, res.String(), http.StatusInternalServerError)
	}

	return nil
}

func (e *ElasticsearchEngine) IndexChannel(rctx request.CTX, channel *model.Channel, userIDs []string, teamMemberIDs []string) *model.AppError {
	// Not implemented yet
	return nil
}

func (e *ElasticsearchEngine) SearchChannels(teamId, userID, term string, isGuest bool, includeDeleted bool) ([]string, *model.AppError) {
	// Not implemented yet
	return nil, nil
}

func (e *ElasticsearchEngine) DeleteChannel(channel *model.Channel) *model.AppError {
	// Not implemented yet
	return nil
}

func (e *ElasticsearchEngine) IndexUser(rctx request.CTX, user *model.User, teamsIds []string, channelsIds []string) *model.AppError {
	// Not implemented yet
	return nil
}

func (e *ElasticsearchEngine) SearchUsersInChannel(teamId, channelId string, restrictedToChannels []string, term string, options *model.UserSearchOptions) ([]string, []string, *model.AppError) {
	// Not implemented yet
	return nil, nil, nil
}

func (e *ElasticsearchEngine) SearchUsersInTeam(teamId string, restrictedToChannels []string, term string, options *model.UserSearchOptions) ([]string, *model.AppError) {
	// Not implemented yet
	return nil, nil
}

func (e *ElasticsearchEngine) DeleteUser(user *model.User) *model.AppError {
	// Not implemented yet
	return nil
}

func (e *ElasticsearchEngine) IndexFile(file *model.FileInfo, channelId string) *model.AppError {
	// Not implemented yet
	return nil
}

func (e *ElasticsearchEngine) PurgeIndexes(rctx request.CTX) *model.AppError {
	if !e.IsActive() {
		return nil
	}

	indexes := []string{PostIndex, UserIndex, FileIndex}
	for _, index := range indexes {
		res, err := e.Client.Indices.Delete([]string{index})
		if err != nil {
			return model.NewAppError("ElasticsearchEngine.PurgeIndexes", "searchengine.elasticsearch.purge_indexes.error", nil, err.Error(), http.StatusInternalServerError)
		}
		defer res.Body.Close()

		if res.IsError() && res.StatusCode != http.StatusNotFound {
			return model.NewAppError("ElasticsearchEngine.PurgeIndexes", "searchengine.elasticsearch.purge_indexes.error", nil, res.String(), http.StatusInternalServerError)
		}
	}

	return nil
}

func (e *ElasticsearchEngine) DataRetentionDeleteIndexes(rctx request.CTX, cutoff time.Time) *model.AppError {
	if !e.IsActive() {
		return nil
	}

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"range": map[string]interface{}{
				"create_at": map[string]interface{}{
					"lte": cutoff.UnixMilli(),
				},
			},
		},
	}

	res, err := e.Client.DeleteByQuery(
		[]string{PostIndex},
		strings.NewReader(toJSON(query)),
		e.Client.DeleteByQuery.WithRefresh(true),
	)
	if err != nil {
		return model.NewAppError("ElasticsearchEngine.DataRetentionDeleteIndexes", "searchengine.elasticsearch.data_retention_delete_indexes.error", nil, err.Error(), http.StatusInternalServerError)
	}
	defer res.Body.Close()

	if res.IsError() {
		return model.NewAppError("ElasticsearchEngine.DataRetentionDeleteIndexes", "searchengine.elasticsearch.data_retention_delete_indexes.error", nil, res.String(), http.StatusInternalServerError)
	}

	return nil
}

func toJSON(data interface{}) string {
	b, _ := json.Marshal(data)
	return string(b)
}