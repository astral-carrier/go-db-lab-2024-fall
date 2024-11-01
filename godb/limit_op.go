package godb

type LimitOp struct {
	// Required fields for parser
	child     Operator
	limitTups Expr
	// Add additional fields here, if needed
}

// Construct a new limit operator. lim is how many tuples to return and child is
// the child operator.
func NewLimitOp(lim Expr, child Operator) *LimitOp {
	return &LimitOp{child: child, limitTups: lim}
}

// Return a TupleDescriptor for this limit.
func (l *LimitOp) Descriptor() *TupleDesc {
	return l.child.Descriptor()
}

// Limit operator implementation. This function should iterate over the results
// of the child iterator, and limit the result set to the first [lim] tuples it
// sees (where lim is specified in the constructor).
func (l *LimitOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	childIter, err := l.child.Iterator(tid)

	if err != nil {
		return nil, err
	}

	limit, limitEvalError := l.limitTups.EvalExpr(nil)

	if limitEvalError != nil {
		return nil, limitEvalError
	}

	outputCount := int64(0)

	return func() (*Tuple, error) {
		// limit reached
		if outputCount >= limit.(IntField).Value {
			return nil, nil
		}

		t, fetchError := childIter()

		if fetchError != nil {
			return nil, fetchError
		}

		if t == nil {
			// iterator exhausted
			return nil, nil
		}

		outputCount++

		return t, nil
	}, nil
}
