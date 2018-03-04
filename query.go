package main

import (
	"errors"
	"math"
)

const (
	NO_MORE   = int64(math.MaxInt64)
	NOT_READY = int64(-1)
)

type Query interface {
	advance(int64) int64
	Next() int64
	GetDocId() int64
}

type QueryBase struct {
	docId int64
}

func (q *QueryBase) GetDocId() int64 {
	return q.docId
}

type Term struct {
	cursor   int64
	postings []uint64
	term     string
	QueryBase
}

func (t *Term) advance(target int64) int64 {
	if t.docId == NO_MORE || t.docId == target || target == NO_MORE {
		t.docId = target
		return t.docId
	}
	if t.cursor < 0 {
		t.cursor = 0
	}

	start := t.cursor
	end := int64(len(t.postings))

	for start < end {
		mid := start + ((end - start) / 2)
		current := int64(t.postings[mid])
		if current == target {
			t.cursor = mid
			t.docId = target
			return t.GetDocId()
		}

		if current < target {
			start = mid + 1
		} else {
			end = mid
		}
	}

	return t.move(start)
}

func (t *Term) move(to int64) int64 {
	t.cursor = to
	if t.cursor >= int64(len(t.postings)) {
		t.docId = NO_MORE
	} else {
		t.docId = int64(t.postings[t.cursor])
	}
	return t.docId
}

func (t *Term) Next() int64 {
	t.cursor++
	return t.move(t.cursor)
}

type BoolQueryBase struct {
	queries []Query
}

func (q *BoolQueryBase) AddSubQuery(sub Query) {
	q.queries = append(q.queries, sub)
}

type BoolOrQuery struct {
	BoolQueryBase
	QueryBase
}

func NewBoolOrQuery(queries []Query) *BoolOrQuery {
	return &BoolOrQuery{
		BoolQueryBase: BoolQueryBase{queries},
		QueryBase:     QueryBase{NOT_READY},
	}
}

func (q *BoolOrQuery) advance(target int64) int64 {
	new_doc := NO_MORE
	n := len(q.queries)
	for i := 0; i < n; i++ {
		sub_query := q.queries[i]
		cur_doc := sub_query.GetDocId()
		if cur_doc < target {
			cur_doc = sub_query.advance(target)
		}

		if cur_doc < new_doc {
			new_doc = cur_doc
		}
	}
	q.docId = new_doc
	return q.docId
}

func (q *BoolOrQuery) Next() int64 {
	new_doc := NO_MORE
	n := len(q.queries)
	for i := 0; i < n; i++ {
		sub_query := q.queries[i]
		cur_doc := sub_query.GetDocId()
		if cur_doc == q.docId {
			cur_doc = sub_query.Next()
		}

		if cur_doc < new_doc {
			new_doc = cur_doc
		}
	}
	q.docId = new_doc
	return new_doc
}

type BoolAndQuery struct {
	BoolQueryBase
	QueryBase
}

func NewBoolAndQuery(queries []Query) *BoolAndQuery {
	return &BoolAndQuery{
		BoolQueryBase: BoolQueryBase{queries},
		QueryBase:     QueryBase{NOT_READY},
	}
}

func (q *BoolAndQuery) nextAndedDoc(target int64) int64 {
	// initial iteration skips queries[0]
	n := len(q.queries)
	for i := 1; i < n; i++ {
		sub_query := q.queries[i]
		if sub_query.GetDocId() < target {
			sub_query.advance(target)
		}

		if sub_query.GetDocId() == target {
			continue
		}

		target = q.queries[0].advance(sub_query.GetDocId())
		i = 0 //restart the loop from the first query
	}
	q.docId = target
	return q.docId
}

func (q *BoolAndQuery) advance(target int64) int64 {
	if len(q.queries) == 0 {
		q.docId = NO_MORE
		return NO_MORE
	}

	return q.nextAndedDoc(q.queries[0].advance(target))
}

func (q *BoolAndQuery) Next() int64 {
	if len(q.queries) == 0 {
		q.docId = NO_MORE
		return NO_MORE
	}

	// XXX: pick cheapest leading query
	return q.nextAndedDoc(q.queries[0].Next())
}

/*

{
   and: [{"or": [{"tag":"b"}]}]
}

*/

func fromJSON(store *StoreItem, input interface{}) (Query, error) {
	mapped, ok := input.(map[string]interface{})
	queries := []Query{}
	if ok {

		if v, ok := mapped["tag"]; ok && v != nil {
			value, ok := v.(string)
			if !ok {
				return nil, errors.New("[tag] must be a string")
			}
			queries = append(queries, store.CreatePostingsList(value).newTermQuery())
		}
		if v, ok := mapped["and"]; ok && v != nil {
			list, ok := v.([]interface{})
			if ok {
				and := NewBoolAndQuery([]Query{})
				for _, subQuery := range list {
					q, err := fromJSON(store, subQuery)
					if err != nil {
						return nil, err
					}
					and.AddSubQuery(q)
				}
				queries = append(queries, and)
			} else {
				return nil, errors.New("[or] takes array of subqueries")
			}
		}

		if v, ok := mapped["or"]; ok && v != nil {
			list, ok := v.([]interface{})
			if ok {
				or := NewBoolOrQuery([]Query{})
				for _, subQuery := range list {
					q, err := fromJSON(store, subQuery)
					if err != nil {
						return nil, err
					}
					or.AddSubQuery(q)
				}
				queries = append(queries, or)
			} else {
				return nil, errors.New("[and] takes array of subqueries")
			}
		}
	}

	if len(queries) == 1 {
		return queries[0], nil
	}

	return NewBoolAndQuery(queries), nil
}
