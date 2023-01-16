package es

import (
	"github.com/olivere/elastic/v7"
	"golang.org/x/net/context"
)

const (
	DefaultClient       = "es-default-client"
	DefaultReadClient   = "es-default-read-client"
	DefaultWriteClient  = "es-default-write-client"
	DefaultVersionType  = "external"
	VersionTypeInternal = "internal"
	DefaultRefresh      = "false"
	RefreshWaitFor      = "wait_for"
	RefreshTrue         = "true"
	DefaultScriptLang   = "painless"
)

type BulkDoc struct {
	ID          string
	Routing     string
	Version     string
	VersionType string
}

type BulkCreateDoc struct {
	BulkDoc
	Doc interface{}
}

type BulkUpsertDoc struct {
	BulkCreateDoc
	update map[string]interface{}
}

type BulkUpdateDoc struct {
	BulkDoc
	update map[string]interface{}
}

func (c *Client) Create(ctx context.Context, indexName, id, routing string, doc interface{}) error {
	// 注意 sdk这里第一个index获取的是*IndexService，即索引服务，第二个Index是指定需要写入的索引名
	indexService := c.Client.Index().Index(indexName).OpType("create")
	if len(id) > 0 {
		indexService.Id(id)
	}
	if len(routing) > 0 {
		indexService.Routing(routing)
	}
	//Refresh setting每隔1s刷新到index buffer里面，刷新os.Cache才能看见
	//false 特定的时间点才能看见
	//wait_for在操作响应之前，等待请求所做的改变通过刷新而变得可见，这并不强迫立即进行刷新，而是等待刷新的发生。Elasticsearch每隔index.refresh_interval(默认每隔1s)就会自动刷新
	_, err := indexService.BodyJson(doc).Refresh(DefaultRefresh).Do(ctx)
	return err
}

func (c *Client) BulkCreate(indexName, id, routing string, doc interface{}) {
	bulkCreateRequest := elastic.NewBulkCreateRequest().Index(indexName).Doc(doc)
	if len(id) > 0 {
		bulkCreateRequest.Id(id)
	}
	if len(routing) > 0 {
		bulkCreateRequest.Routing(routing)
	}
	c.BulkProcessor.Add(bulkCreateRequest)
}

func (c *Client) BulkCreateDocs(ctx context.Context, indexName string, docs []*BulkCreateDoc) (*elastic.BulkResponse, error) {
	bulkService := c.Client.Bulk().ErrorTrace(true)
	for _, doc := range docs {
		// 索引存在报错
		bulkCreateRequest := elastic.NewBulkCreateRequest().Index(indexName)
		if len(doc.ID) > 0 {
			bulkCreateRequest.Id(doc.ID)
		}
		if len(doc.Routing) > 0 {
			bulkCreateRequest.Routing(doc.Routing)
		}
		bulkService.Add(bulkCreateRequest)
	}
	return bulkService.Do(ctx)
}

func (c *Client) BulkCreateWithVersion(ctx context.Context, indexName, id, routing string, version int64, doc interface{}) {
	bulkCreateRequest := elastic.NewBulkIndexRequest().Index(indexName).Doc(doc).VersionType(DefaultVersionType).Version(version)
	if len(id) > 0 {
		bulkCreateRequest.Id(id)
	}
	if len(routing) > 0 {
		bulkCreateRequest.Routing(routing)
	}
	c.BulkProcessor.Add(bulkCreateRequest)
}

func (c *Client) Delete(ctx context.Context, indexName, id, routing string) error {
	deleteService := c.Client.Delete().Index(indexName).Id(id).Refresh(DefaultRefresh)
	if len(routing) > 0 {
		deleteService.Routing(routing)
	}
	_, err := deleteService.Do(ctx)
	return err
}

func (c *Client) DeleteRefresh(ctx context.Context, indexName, id, routing string) error {
	deleteService := c.Client.Delete().Index(indexName).Id(id).Refresh(RefreshTrue)
	if len(routing) > 0 {
		deleteService.Routing(routing)
	}
	_, err := deleteService.Do(ctx)
	return err
}

func (c *Client) DeleteWithVersion(ctx context.Context, indexName, id, routing string, version int64) error {
	deleteService := c.Client.Delete().Index(indexName).Id(id).VersionType(DefaultVersionType).Version(version).Refresh(DefaultRefresh)
	if len(routing) > 0 {
		deleteService.Routing(routing)
	}
	_, err := deleteService.Do(ctx)
	return err
}

func (c *Client) DeleteByQuery(ctx context.Context, indexName, id, routing string, query elastic.Query) error {
	//产生冲突的时候继续删除文档
	deleteService := c.Client.DeleteByQuery(indexName).Query(query).ProceedOnVersionConflict().Refresh(DefaultRefresh)
	if len(routing) > 0 {
		deleteService.Routing(routing)
	}
	_, err := deleteService.Do(ctx)
	return err
}

func (c *Client) BulkDelete(indexName, id, routing string, version int64) {
	bulkDeleteRequest := elastic.NewBulkDeleteRequest().Index(indexName).VersionType(DefaultVersionType).Version(version).Id(id)
	if len(routing) > 0 {
		bulkDeleteRequest.Routing(routing)
	}
	c.BulkProcessor.Add(bulkDeleteRequest)

}

func (c *Client) BulkDeleteWithVersion(indexName, id, routing string, version int64) {
	bulkDeleteRequest := elastic.NewBulkDeleteRequest().Index(indexName).Id(id).VersionType(DefaultVersionType).Version(version)
	if len(routing) > 0 {
		bulkDeleteRequest.Routing(routing)
	}
	c.BulkProcessor.Add(bulkDeleteRequest)
}

func (c *Client) Update(ctx context.Context, indexName, id, routing string, update map[string]interface{}) error {
	updateService := c.Client.Update().Index(indexName).Id(id).Refresh(DefaultRefresh)
	if len(routing) > 0 {
		updateService.Routing(routing)
	}
	_, err := updateService.Doc(update).Do(ctx)
	return err
}

func (c *Client) UpdateRefresh(ctx context.Context, indexName, id, routing string, update map[string]interface{}) error {
	updateService := c.Client.Update().Index(indexName).Id(id).Refresh(RefreshTrue)
	if len(routing) > 0 {
		updateService.Routing(routing)
	}
	_, err := updateService.Doc(update).Do(ctx)
	return err
}

func (c *Client) UpdateQuery(ctx context.Context, indexName string, routings []string, query elastic.Query, script string, scriptParams map[string]interface{}) (*elastic.BulkIndexByScrollResponse, error) {
	updateByQueryService := c.Client.UpdateByQuery(indexName).Query(query).Script(elastic.NewScript(script).Params(scriptParams).
		Lang(DefaultScriptLang)).Refresh(DefaultRefresh).ProceedOnVersionConflict()
	if len(routings) > 0 {
		updateByQueryService.Routing(routings...)
	}
	return updateByQueryService.Do(ctx)
}

func (c *Client) BulkUpdate(indexName, id, routing string, update map[string]interface{}) {
	bulkService := elastic.NewBulkUpdateRequest().Index(indexName).Id(id).Doc(update)
	if len(routing) > 0 {
		bulkService.Routing(routing)
	}
	c.BulkProcessor.Add(bulkService)
}

func (c *Client) BulkUpdateDocs(ctx context.Context, index string, updates []*BulkUpdateDoc) (*elastic.BulkResponse, error) {
	bulkService := c.Client.Bulk().ErrorTrace(true).Refresh(DefaultRefresh)
	for _, update := range updates {
		doc := elastic.NewBulkUpdateRequest().Id(update.ID).Doc(update.update)
		if len(update.Routing) > 0 {
			doc.Routing(update.Routing)
		}
		bulkService.Add(doc)
	}
	return bulkService.Do(ctx)
}

func (c *Client) UpsertWithVersion(ctx context.Context, indexName, id, routing string, doc interface{}, version int64) error {
	indexService := c.Client.Index().OpType("index").Index(indexName).Id(id).Refresh(DefaultRefresh).Version(version).VersionType(DefaultVersionType)
	if len(routing) > 0 {
		indexService.Routing(routing)
	}
	_, err := indexService.BodyJson(doc).Do(ctx)
	return err
}

func (c *Client) Upsert(ctx context.Context, indexName, id, routing string, update map[string]interface{}, doc interface{}) error {
	updateService := c.Client.Update().Index(indexName).Id(id).Refresh(DefaultRefresh).DocAsUpsert(true)
	if len(routing) > 0 {
		updateService.Routing(routing)
	}
	_, err := updateService.Doc(update).Upsert(doc).Do(ctx)
	return err
}

func (c *Client) BulkUpsert(indexName, id, routing string, update map[string]interface{}, doc interface{}) {
	bulkUpdateRequest := elastic.NewBulkUpdateRequest().Index(indexName).Doc(update).Id(id).Upsert(doc).DocAsUpsert(true)
	if len(routing) > 0 {
		bulkUpdateRequest.Routing(routing)
	}
	c.BulkProcessor.Add(bulkUpdateRequest)
}

// UpsertBulk 批量upsert
func (c *Client) BulkUpsertDocs(ctx context.Context, index string, docs []*BulkUpsertDoc) (*elastic.BulkResponse, error) {
	bulkService := c.Client.Bulk().ErrorTrace(true).Refresh(DefaultRefresh)
	for _, doc := range docs {
		index := elastic.NewBulkUpdateRequest().Id(doc.ID).Doc(doc.update).Upsert(doc.Doc).DocAsUpsert(true)
		if len(doc.Routing) > 0 {
			index.Routing(doc.Routing)
		}
		bulkService.Add(index)
	}
	return bulkService.Do(ctx)
}
