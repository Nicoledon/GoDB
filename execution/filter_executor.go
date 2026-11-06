package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// FilterExecutor filters tuples from its child executor based on a predicate.
type FilterExecutor struct {
	// Fill me in!
	plan  *planner.FilterNode
	child Executor
	ctx   *ExecutorContext
}

// NewFilter creates a new FilterExecutor executor.
func NewFilter(plan *planner.FilterNode, child Executor) *FilterExecutor {
	return &FilterExecutor{
		plan:  plan,
		child: child,
	}
}

func (e *FilterExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

// Init initializes the child.
func (e *FilterExecutor) Init(context *ExecutorContext) error {
	e.ctx = context
	err := e.child.Init(context)
	if err != nil {
		return err
	}
	return nil
}

func (e *FilterExecutor) Next() bool {
	for e.child.Next() {
		if e.plan.Predicate.Eval(e.child.Current()).IntValue() == 1 {
			return true
		}
	}
	return false
}

func (e *FilterExecutor) Current() storage.Tuple {
	return e.child.Current()
}

func (e *FilterExecutor) Error() error {
	return nil
}

func (e *FilterExecutor) Close() error {
	return nil
}
