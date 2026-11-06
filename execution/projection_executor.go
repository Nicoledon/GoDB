package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// ProjectionExecutor evaluates a list of expressions on the input tuples
// and produces a new tuple containing the results of those expressions.
type ProjectionExecutor struct {
	// Fill me in!
	plan  *planner.ProjectionNode
	child Executor
	ctx   *ExecutorContext
}

// NewProjectionExecutor creates a new ProjectionExecutor.
func NewProjectionExecutor(plan *planner.ProjectionNode, child Executor) *ProjectionExecutor {
	return &ProjectionExecutor{
		plan:  plan,
		child: child,
	}
}

func (e *ProjectionExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *ProjectionExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	err := e.child.Init(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (e *ProjectionExecutor) Next() bool {
	return e.child.Next()
}

func (e *ProjectionExecutor) Current() storage.Tuple {
	tuple := e.child.Current()
	tuples := make([]common.Value, 0)
	for _, expr := range e.plan.Expressions {
		tuples = append(tuples, expr.Eval(tuple))
	}
	return storage.FromValues(tuples...)
}

func (e *ProjectionExecutor) Error() error {
	return nil
}

func (e *ProjectionExecutor) Close() error {
	return nil
}
