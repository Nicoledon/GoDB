package execution

import (
	"container/heap"

	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

type PriorityQueue []*SortMetaData

// TopNExecutor optimizes "ORDER BY ... LIMIT N" queries.
//
// This should allow an optimized implementation that avoids sorting ALL tuples (O(M log M)).
type TopNExecutor struct {
	ctx     *ExecutorContext
	plan    *planner.TopNNode
	child   Executor
	cnt     int
	current storage.Tuple
	queue   *PriorityQueue
	count   int
	key     *SortMetaData
	cap     int
	// Fill me in!
}

func (pq PriorityQueue) Len() int {
	return len(pq)
}

func (pq PriorityQueue) Less(i, j int) bool {
	return Compare(*pq[j], *pq[i])
}
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x any) {
	item := x.(*SortMetaData) // 断言成具体类型
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[:n-1]
	return item
}

func NewTopNExecutor(plan *planner.TopNNode, child Executor) *TopNExecutor {
	return &TopNExecutor{
		plan:  plan,
		child: child,
		queue: &PriorityQueue{},
	}
}

func (e *TopNExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *TopNExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	e.child.Init(ctx)
	heap.Init(e.queue)
	return nil
}
func (e *TopNExecutor) Next() bool {
	if e.count == 0 {
		for e.child.Next() {
			item := e.child.Current().DeepCopy(storage.NewRawTupleDesc(e.child.PlanNode().OutputSchema()))
			values := make([]common.Value, 0)
			directs := make([]planner.SortDirection, 0)
			for _, expr := range e.plan.OrderBy {
				values = append(values, expr.Expr.Eval(item))
				directs = append(directs, expr.Direction)
			}
			tuple := SortMetaData{item, values, directs}
			heap.Push(e.queue, &tuple)
		}
		e.count += 1
	}
	if e.cnt == e.plan.Limit {
		return false
	}
	if e.queue.Len() == 0 {
		return false
	}
	return true
}

func (e *TopNExecutor) Current() storage.Tuple {
	item := heap.Pop(e.queue).(*SortMetaData)
	e.cnt += 1
	return item.tuple
}

func (e *TopNExecutor) Error() error {
	return nil
}

func (e *TopNExecutor) Close() error {
	return nil
}
