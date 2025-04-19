// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package elasticsearch

import (
	"testing"
	"time"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/shared/request"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestElasticsearchEngine(t *testing.T) {
	cfg := &model.Config{}
	cfg.SetDefaults()

	t.Run("NewElasticsearchEngine", func(t *testing.T) {
		engine := NewElasticsearchEngine(cfg)
		assert.NotNil(t, engine)
		assert.Equal(t, cfg, engine.Config)
		assert.False(t, engine.IsActive())
	})

	t.Run("Start", func(t *testing.T) {
		engine := NewElasticsearchEngine(cfg)

		// Test with indexing disabled
		cfg.ElasticsearchSettings.EnableIndexing = model.NewBool(false)
		err := engine.Start()
		assert.Nil(t, err)
		assert.False(t, engine.IsActive())

		// Test with indexing enabled but invalid connection
		cfg.ElasticsearchSettings.EnableIndexing = model.NewBool(true)
		cfg.ElasticsearchSettings.ConnectionUrl = model.NewString("invalid://localhost:9200")
		err = engine.Start()
		assert.NotNil(t, err)
		assert.False(t, engine.IsActive())
	})

	t.Run("Stop", func(t *testing.T) {
		engine := NewElasticsearchEngine(cfg)
		err := engine.Stop()
		assert.Nil(t, err)
		assert.False(t, engine.IsActive())
	})

	t.Run("IsEnabled", func(t *testing.T) {
		engine := NewElasticsearchEngine(cfg)

		cfg.ElasticsearchSettings.EnableIndexing = model.NewBool(false)
		assert.False(t, engine.IsIndexingEnabled())

		cfg.ElasticsearchSettings.EnableIndexing = model.NewBool(true)
		assert.True(t, engine.IsIndexingEnabled())
	})

	t.Run("GetName", func(t *testing.T) {
		engine := NewElasticsearchEngine(cfg)
		assert.Equal(t, "elasticsearch", engine.GetName())
	})

	t.Run("TestConfig", func(t *testing.T) {
		engine := NewElasticsearchEngine(cfg)

		// Test with indexing disabled
		cfg.ElasticsearchSettings.EnableIndexing = model.NewBool(false)
		err := engine.TestConfig(request.TestContext(t), cfg)
		assert.Nil(t, err)

		// Test with indexing enabled but invalid connection
		cfg.ElasticsearchSettings.EnableIndexing = model.NewBool(true)
		cfg.ElasticsearchSettings.ConnectionUrl = model.NewString("invalid://localhost:9200")
		err = engine.TestConfig(request.TestContext(t), cfg)
		assert.NotNil(t, err)
	})
}

func TestElasticsearchEngineIndexing(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cfg := &model.Config{}
	cfg.SetDefaults()
	cfg.ElasticsearchSettings.EnableIndexing = model.NewBool(true)
	cfg.ElasticsearchSettings.ConnectionUrl = model.NewString("http://localhost:9200")

	engine := NewElasticsearchEngine(cfg)
	err := engine.Start()
	require.Nil(t, err)
	defer engine.Stop()

	t.Run("IndexPost", func(t *testing.T) {
		post := &model.Post{
			Id:        model.NewId(),
			ChannelId: model.NewId(),
			UserId:    model.NewId(),
			Message:   "test message",
			CreateAt:  model.GetMillis(),
			UpdateAt:  model.GetMillis(),
		}

		err := engine.IndexPost(post, "team1")
		assert.Nil(t, err)

		// Wait for indexing
		time.Sleep(1 * time.Second)

		// Search for the post
		channels := model.ChannelList{
			&model.Channel{
				Id: post.ChannelId,
			},
		}
		searchParams := []*model.SearchParams{
			{
				Terms: "test",
			},
		}

		results, matches, err := engine.SearchPosts(channels, searchParams, 0, 10)
		assert.Nil(t, err)
		assert.Len(t, results, 1)
		assert.Equal(t, post.Id, results[0])
		assert.Contains(t, matches, post.Id)
	})

	t.Run("DeletePost", func(t *testing.T) {
		post := &model.Post{
			Id:        model.NewId(),
			ChannelId: model.NewId(),
			UserId:    model.NewId(),
			Message:   "test message for deletion",
			CreateAt:  model.GetMillis(),
			UpdateAt:  model.GetMillis(),
		}

		err := engine.IndexPost(post, "team1")
		assert.Nil(t, err)

		// Wait for indexing
		time.Sleep(1 * time.Second)

		err = engine.DeletePost(post)
		assert.Nil(t, err)

		// Wait for deletion
		time.Sleep(1 * time.Second)

		// Search for the deleted post
		channels := model.ChannelList{
			&model.Channel{
				Id: post.ChannelId,
			},
		}
		searchParams := []*model.SearchParams{
			{
				Terms: "deletion",
			},
		}

		results, _, err := engine.SearchPosts(channels, searchParams, 0, 10)
		assert.Nil(t, err)
		assert.Len(t, results, 0)
	})

	t.Run("DeleteChannelPosts", func(t *testing.T) {
		channelId := model.NewId()
		post1 := &model.Post{
			Id:        model.NewId(),
			ChannelId: channelId,
			UserId:    model.NewId(),
			Message:   "test message channel 1",
			CreateAt:  model.GetMillis(),
			UpdateAt:  model.GetMillis(),
		}
		post2 := &model.Post{
			Id:        model.NewId(),
			ChannelId: channelId,
			UserId:    model.NewId(),
			Message:   "test message channel 2",
			CreateAt:  model.GetMillis(),
			UpdateAt:  model.GetMillis(),
		}

		err := engine.IndexPost(post1, "team1")
		assert.Nil(t, err)
		err = engine.IndexPost(post2, "team1")
		assert.Nil(t, err)

		// Wait for indexing
		time.Sleep(1 * time.Second)

		err = engine.DeleteChannelPosts(request.TestContext(t), channelId)
		assert.Nil(t, err)

		// Wait for deletion
		time.Sleep(1 * time.Second)

		// Search for the deleted posts
		channels := model.ChannelList{
			&model.Channel{
				Id: channelId,
			},
		}
		searchParams := []*model.SearchParams{
			{
				Terms: "channel",
			},
		}

		results, _, err := engine.SearchPosts(channels, searchParams, 0, 10)
		assert.Nil(t, err)
		assert.Len(t, results, 0)
	})
}