package es

import (
	"context"
	"encoding/json"
	"github.com/olivere/elastic/v7"
	"io"
	"strings"
)

type Mget struct {
	Index   string
	ID      string
	Routing string
}

type queryOption struct {
	Orders               []map[string]bool
	Highlight            *elastic.Highlight
	Profile              bool
	EnableDSL            bool     //是否打印查询语句
	ExcludeFields        []string //查询不返回字段
	IncludeFields        []string
	SlowQueryMillisecond int64
	Preference           string
	FetchSource          *bool
}
type QueryOption func(queryOption *queryOption)

const DefaultPreference = "_local"

func WithOrders(orders []map[string]bool) QueryOption {
	return func(opt *queryOption) {
		opt.Orders = orders
	}
}
func WithHighlight(highlight *elastic.Highlight) QueryOption {
	return func(opt *queryOption) {
		opt.Highlight = highlight
	}
}

func WithProfile(profile bool) QueryOption {
	return func(opt *queryOption) {
		opt.Profile = profile
	}
}

func WithEnableDSL(enableDSL bool) QueryOption {
	return func(opt *queryOption) {
		opt.EnableDSL = enableDSL
	}
}
func WithIncludeFields(includeFields []string) QueryOption {
	return func(opt *queryOption) {
		opt.IncludeFields = includeFields
	}
}

func WithExcludeFields(excludeFields []string) QueryOption {
	return func(opt *queryOption) {
		opt.ExcludeFields = excludeFields
	}
}

func WithSlowQueryMillisecond(slowQueryLogMillisecond int64) QueryOption {
	return func(opt *queryOption) {
		opt.SlowQueryMillisecond = slowQueryLogMillisecond
	}
}

func WithPreference(preference string) QueryOption {
	return func(opt *queryOption) {
		opt.Preference = preference
	}
}

func WithFetchSource(fetchSource bool) QueryOption {
	return func(opt *queryOption) {
		opt.FetchSource = &fetchSource
	}
}

func (c *Client) Get(ctx context.Context, indexName, id, routing string) (*elastic.GetResult, error) {
	getService := c.Client.Get().Index(indexName).Id(id).Preference(DefaultPreference)
	if len(routing) > 0 {
		getService.Routing(routing)
	}
	return getService.Do(ctx)
}

func (c *Client) Query(ctx context.Context, indexName string, routes []string, query elastic.Query, from, size int, options ...QueryOption) (*elastic.SearchResult, error) {
	queryOpt := &queryOption{}
	for _, f := range options {
		if f != nil {
			f(queryOpt)
		}
	}
	//设置Source
	fetchSource := true
	if queryOpt.FetchSource != nil && *queryOpt.FetchSource == false {
		fetchSource = false
	}
	fetchSourceContext := elastic.NewFetchSourceContext(fetchSource)
	if len(queryOpt.IncludeFields) > 0 {
		fetchSourceContext.Include(queryOpt.IncludeFields...)
	}
	if len(queryOpt.ExcludeFields) > 0 {
		fetchSourceContext.Exclude(queryOpt.ExcludeFields...)
	}
	//构造查询条件
	searchSource := elastic.NewSearchSource()
	searchSource = searchSource.FetchSourceContext(fetchSourceContext).Query(query).From(from).Size(size)
	if len(queryOpt.Orders) > 0 {
		for _, orderM := range queryOpt.Orders {
			for field, order := range orderM {
				searchSource.Sort(field, order)
			}
		}
	}
	if queryOpt.Highlight != nil {
		searchSource.Highlight(queryOpt.Highlight)
	}
	searchSource.Profile(queryOpt.Profile)

	searchService := c.Client.Search(indexName).SearchSource(searchSource).IgnoreUnavailable(true).Preference(DefaultPreference)
	if len(routes) > 0 {
		searchService.Routing(routes...)
	}
	if len(queryOpt.Preference) > 0 {
		searchService.Preference(queryOpt.Preference)
	} else {
		searchService.Preference(DefaultPreference)
	}

	res, err := searchService.Do(ctx)
	//获取查询语句
	src, _ := searchSource.Source()
	data, _ := json.Marshal(src)
	rs := strings.Join(routes, ",")
	if c.DebugMode || c.QueryLogEnable || queryOpt.EnableDSL {
		EStdLogger.Print("DSL : ", string(data), "routing: ", rs)
	}
	if queryOpt.SlowQueryMillisecond > 0 && res != nil && res.TookInMillis >= queryOpt.SlowQueryMillisecond {
		EStdLogger.Print("slow query DSL : ", string(data), "routing: ", rs)
	}
	return res, err
}

func (c *Client) ScrollQuery(ctx context.Context, index []string, typeStr string, query elastic.Query, size int, routes []string, callback func(res *elastic.SearchResult, err error), options ...QueryOption) {
	queryOpt := &queryOption{}
	for _, f := range options {
		if f != nil {
			f(queryOpt)
		}
	}
	fetchSource := true
	if queryOpt.FetchSource != nil && *queryOpt.FetchSource == false {
		fetchSource = false
	}
	fetchSourceContext := elastic.NewFetchSourceContext(fetchSource)
	searchSource := elastic.NewSearchSource()
	searchSource = searchSource.FetchSourceContext(fetchSourceContext).Query(query)

	if len(queryOpt.Orders) > 0 {
		for _, orderM := range queryOpt.Orders {
			for field, order := range orderM {
				searchSource.Sort(field, order)
			}
		}
	}
	if queryOpt.Highlight != nil {
		searchSource.Highlight(queryOpt.Highlight)
	}
	searchSource.Profile(queryOpt.Profile)
	src, _ := searchSource.Source()
	data, _ := json.Marshal(src)
	rs := strings.Join(routes, ",")
	if c.DebugMode || c.QueryLogEnable || queryOpt.EnableDSL {
		EStdLogger.Print("DSL : ", string(data), "routing: ", rs)
	}
	scrollService := c.Client.Scroll(index...).SearchSource(searchSource).Size(size).Preference(DefaultPreference)
	if len(routes) > 0 {
		scrollService.Routing(routes...)
	}
	if len(queryOpt.Preference) > 0 {
		scrollService.Preference(queryOpt.Preference)
	} else {
		scrollService.Preference(DefaultPreference)
	}
	//scroll保存在ES集群中的上下文信息会占用大量内存资源，虽然会在一段时间后自动清理，当我们知道scroll结束后,
	//需要手动调用clear释放资源
	defer scrollService.Clear(ctx)
	for {
		res, err := scrollService.Do(ctx)
		if err == io.EOF {
			break
		}
		if queryOpt.SlowQueryMillisecond > 0 && res != nil && res.TookInMillis >= queryOpt.SlowQueryMillisecond {
			EStdLogger.Print("slow query DSL : ", string(data), "routing: ", rs)
		}
		if res == nil {
			EStdLogger.Print("nil results !")
			break
		}
		if res.Hits == nil {
			EStdLogger.Print("expected results.Hits != nil; got nil")
		}

		if len(res.Hits.Hits) == 0 {
			break
		}
		callback(res, err)
	}

}
