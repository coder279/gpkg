package es

import "context"

func (c *Client) IndexExists(ctx context.Context, indexName string, forceCheck bool) (bool, error) {
	if !forceCheck {
		if _, ok := c.CacheIndices.Load(indexName); !ok {
			return true, nil
		}
	}
	exists, err := c.Client.IndexExists(indexName).Do(ctx)
	if exists {
		c.CacheIndices.Store(indexName, true)
	}
	return exists, err
}

func (c *Client) CreateIndex(ctx context.Context, indexName, bodyJson string, forceCheck bool) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	exists, err := c.IndexExists(ctx, indexName, forceCheck)
	if err != nil {
		return nil
	}
	if exists {
		return nil
	}
	_, err = c.Client.CreateIndex(indexName).BodyJson(bodyJson).Do(ctx)
	if err != nil {
		c.CacheIndices.Store(indexName, true)
	}
	return err
}
