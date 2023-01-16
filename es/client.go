package es

import (
	"crypto/tls"
	"encoding/json"
	"github.com/olivere/elastic/v7"
	"golang.org/x/net/context"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

var clients map[string]*Client

var EStdLogger stdLogger

type stdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

type Client struct {
	Name           string
	Urls           []string
	QueryLogEnable bool
	Username       string
	password       string
	Bulk           *Bulk
	Client         *elastic.Client
	BulkProcessor  *elastic.BulkProcessor
	DebugMode      bool
	CacheIndices   sync.Map
	lock           sync.Mutex
}

type Bulk struct {
	Name          string
	Workers       int                   //Bulk处理线程数
	FlushInterval time.Duration         //刷新频率
	ActionSize    int                   //每次提交的文档数
	RequestSize   int                   //每次提交文档大小
	AfterFunc     elastic.BulkAfterFunc //回调函数
	Ctx           context.Context
}

func DefaultBulk() *Bulk {
	return &Bulk{
		Workers:       3,
		FlushInterval: 1,
		ActionSize:    500,
		RequestSize:   5 << 20,
		AfterFunc:     defaultBulkFunc,
		Ctx:           context.Background(),
	}
}

func defaultBulkFunc(executionId int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	if err != nil || (response != nil && response.Errors) {
		res, _ := json.Marshal(response)
		EStdLogger.Printf("executionId: %d ;requests : %v; response : %s ; err : %+v", executionId, requests, res, err)
	}
}

func InitClient(clientName string, urls []string, username string, password string) error {
	if clients == nil {
		clients = make(map[string]*Client, 0)
	}
	client := &Client{
		Urls:           urls,
		QueryLogEnable: false,
		Username:       username,
		password:       password,
		Bulk:           DefaultBulk(),
		CacheIndices:   sync.Map{},
		lock:           sync.Mutex{},
	}
	client.Bulk.Name = clientName
	options := getBaseOptions(username, password, urls...)
	err := client.newClient(options)
	if err != nil {
		return err
	}
	clients[clientName] = client
	return nil
}

func InitSimpleClient(urls []string, username, password string) error {
	esClient, err := elastic.NewSimpleClient(
		elastic.SetURL(urls...),
		elastic.SetBasicAuth(username, password),
		elastic.SetErrorLog(EStdLogger),
	)
	if err != nil {
		return err
	}
	client := &Client{
		Name:           SimpleClient,
		Urls:           urls,
		QueryLogEnable: false, //默认不允许打开日志
		Username:       username,
		password:       password,
		Bulk:           DefaultBulk(),
		CacheIndices:   sync.Map{},
		lock:           sync.Mutex{},
	}
	client.Client = esClient
	client.Bulk.Name = client.Name
	client.BulkProcessor, err = esClient.BulkProcessor().
		Name(client.Bulk.Name).
		Workers(client.Bulk.Workers).
		BulkActions(client.Bulk.ActionSize).
		BulkSize(client.Bulk.RequestSize).
		FlushInterval(client.Bulk.FlushInterval).
		Stats(true).
		After(client.Bulk.AfterFunc).
		Do(client.Bulk.Ctx)
	if err != nil {
		EStdLogger.Print("init bulkProcessor error ", err)
	}
	if clients == nil {
		clients = make(map[string]*Client, 0)
	}
	clients[SimpleClient] = client
	return nil
}

func getDefaultClient() *http.Client {
	tr := &http.Transport{
		DisableKeepAlives: true,
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
	}
	return &http.Client{Transport: tr}
}

func InitClientWithOptions(clientName string, urls []string, username string, password string, options ...Option) error {
	if clients == nil {
		clients = make(map[string]*Client, 0)
	}
	client := &Client{
		Urls:           urls,
		QueryLogEnable: false,
		Username:       username,
		password:       password,
		Bulk:           DefaultBulk(),
		CacheIndices:   sync.Map{},
		lock:           sync.Mutex{},
	}
	opt := &option{}
	for _, f := range options {
		if f != nil {
			f(opt)
		}
	}
	esOptions := getBaseOptions(username, password, urls...)

	if opt.DebugMode {
		esOptions = append(esOptions, elastic.SetInfoLog(EStdLogger))
	}
	if len(opt.Scheme) > 0 {
		esOptions = append(esOptions, elastic.SetScheme(opt.Scheme))
		esOptions = append(esOptions, elastic.SetHttpClient(getDefaultClient()))
		esOptions = append(esOptions, elastic.SetHealthcheck(false))
	}

	client.QueryLogEnable = opt.QueryLogEnable
	client.Bulk = opt.Bulk
	if client.Bulk == nil {
		client.Bulk = DefaultBulk()
	}
	err := client.newClient(esOptions)
	if err != nil {
		return err
	}
	clients[clientName] = client
	return nil
}

func (c *Client) newClient(options []elastic.ClientOptionFunc) error {
	client, err := elastic.NewClient(options...)
	if err != nil {
		return err
	}
	c.Client = client

	if c.Bulk.Name == "" {
		c.Bulk.Name = c.Name
	}

	if c.Bulk.Workers <= 0 {
		c.Bulk.Workers = 1
	}

	//参数合理性校验
	if c.Bulk.RequestSize > 100*1024*1024 {
		EStdLogger.Print("Bulk RequestSize must be smaller than 100MB; it will be ignored.")
		c.Bulk.RequestSize = 100 * 1024 * 1024
	}

	if c.Bulk.ActionSize >= 10000 {
		EStdLogger.Print("Bulk ActionSize must be smaller than 10000; it will be ignored.")
		c.Bulk.ActionSize = 10000
	}

	if c.Bulk.FlushInterval >= 60 {
		EStdLogger.Print("Bulk FlushInterval must be smaller than 60s; it will be ignored.")
		c.Bulk.FlushInterval = time.Second * 60
	}
	if c.Bulk.AfterFunc == nil {
		c.Bulk.AfterFunc = defaultBulkFunc
	}
	if c.Bulk.Ctx == nil {
		c.Bulk.Ctx = context.Background()
	}

	c.BulkProcessor, err = c.Client.BulkProcessor().
		Name(c.Bulk.Name).
		Workers(c.Bulk.Workers).
		BulkActions(c.Bulk.ActionSize).
		BulkSize(c.Bulk.RequestSize).
		FlushInterval(c.Bulk.FlushInterval).
		Stats(true).
		After(c.Bulk.AfterFunc).
		Do(c.Bulk.Ctx)
	if err != nil {
		EStdLogger.Print("init bulkProcessor error ", err)
	}
	return nil
}

func getBaseOptions(username, password string, urls ...string) []elastic.ClientOptionFunc {
	options := make([]elastic.ClientOptionFunc, 0)
	options = append(options, elastic.SetURL(urls...))
	options = append(options, elastic.SetBasicAuth(username, password))
	options = append(options, elastic.SetHealthcheckTimeoutStartup(15*time.Second))
	//开启Sniff，SDK会定期(默认15分钟一次)嗅探集群中全部节点，将全部节点都加入到连接列表中，
	//后续新增的节点也会自动加入到可连接列表，但实际生产中我们可能会设置专门的协调节点，所以默认不开启嗅探
	options = append(options, elastic.SetSniff(false))
	options = append(options, elastic.SetErrorLog(EStdLogger))
	return options
}

type Option func(*option)
type option struct {
	QueryLogEnable            bool
	GlobalSlowQueryMillSecond int64
	Bulk                      *Bulk
	DebugMode                 bool
	Scheme                    string
}

const (
	SimpleClient = "simple-es-client"
)

func init() {
	EStdLogger = log.New(os.Stdout, "[res]", log.LstdFlags|log.Lshortfile)
}

func WithQueryLogEnable(enable bool) Option {
	return func(o *option) {
		o.QueryLogEnable = enable
	}
}

func WithSlowQueryLogMilliseconde(globalSlowQueryMillSecond int64) Option {
	return func(o *option) {
		o.GlobalSlowQueryMillSecond = globalSlowQueryMillSecond
	}
}

func WithScheme(scheme string) Option {
	return func(o *option) {
		o.Scheme = scheme
	}
}

func WithBulk(bulk *Bulk) Option {
	return func(o *option) {
		o.Bulk = bulk
	}
}

func CloseAll() {
	for _, c := range clients {
		if c != nil {
			err := c.BulkProcessor.Close()
			if err != nil {
				EStdLogger.Print("bulk close error", err)
			}
		}
	}
}

func (c *Client) AddIndexCache(indexName ...string) {
	for _, index := range indexName {
		c.CacheIndices.Store(index, true)
	}
}

func (c *Client) DeleteIndexCache(indexName ...string) {
	for _, index := range indexName {
		c.CacheIndices.Delete(index)
	}
}

func (c *Client) Close() error {
	return c.BulkProcessor.Close()
}
