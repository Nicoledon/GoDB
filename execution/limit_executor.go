package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// LimitExecutor limits the number of tuples returned by the child executor.
type LimitExecutor struct {
	// Fill me in!
	ctx   *ExecutorContext
	plan  *planner.LimitNode
	child Executor
	count int
}

func NewLimitExecutor(plan *planner.LimitNode, child Executor) *LimitExecutor {
	return &LimitExecutor{
		plan:  plan,
		child: child,
		count: 0,
	}
}

func (e *LimitExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *LimitExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	err := e.child.Init(ctx)
	e.count = 0
	if err != nil {
		return err
	}
	return nil
}

func (e *LimitExecutor) Next() bool {
	if e.child.Next() && e.count < e.plan.Limit {
		e.count += 1
		return true
	}
	return false
}

func (e *LimitExecutor) Current() storage.Tuple {
	return e.child.Current()
}

func (e *LimitExecutor) Error() error {
	return nil
}

func (e *LimitExecutor) Close() error {
	return nil
}
