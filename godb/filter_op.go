package godb

type Filter struct {
	op    BoolOp
	left  Expr
	right Expr
	child Operator
}

// Construct a filter operator on ints.
func NewFilter(constExpr Expr, op BoolOp, field Expr, child Operator) (*Filter, error) {
	return &Filter{op, field, constExpr, child}, nil
}

// Return a TupleDescriptor for this filter op.
func (f *Filter) Descriptor() *TupleDesc {
	return &TupleDesc{[]FieldType{f.left.GetExprType()}}
}

// Filter operator implementation. This function should iterate over the results
// of the child iterator and return a tuple if it satisfies the predicate.
//
// HINT: you can use [types.evalPred] to compare two values.
func (f *Filter) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	childIter, childIterError := f.child.Iterator(tid)

	if childIterError != nil {
		return nil, childIterError
	}
	if childIter == nil {
		return nil, GoDBError{MalformedDataError, "child iter unexpectedly nil"}
	}

	return func() (*Tuple, error) {
		for t, err := childIter(); t != nil || err != nil; t, err = childIter() {
			if err != nil {
				return nil, err
			}
			if t == nil {
				return nil, nil
			}

			leftValue, leftEvalError := f.left.EvalExpr(t)

			if leftEvalError != nil {
				return nil, leftEvalError
			}

			rightValue, rightEvalError := f.right.EvalExpr(nil)

			if rightEvalError != nil {
				return nil, rightEvalError
			}

			if leftValue.EvalPred(rightValue, f.op) {
				return t, nil
			}
		}

		return nil, nil
	}, nil
}
